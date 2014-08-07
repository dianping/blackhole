package com.dp.blackhole.http;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.MethodNotSupportedException;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.log4j.Logger;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.SimpleConnection;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes.LxcConfRes;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.QuitAndCleanPB.InstanceGroup;
import com.dp.blackhole.supervisor.ConfigManager;
import com.dp.blackhole.supervisor.Supervisor;

public class HttpPaaSLogout extends HttpAbstractHandler implements HttpRequestHandler {
    private static Logger LOG = Logger.getLogger(HttpPaaSLogout.class);
    private ConfigManager configManager;
    private Supervisor supervisor;
    
    public HttpPaaSLogout(ConfigManager configManger, HttpClientSingle httpClient) {
        this.configManager = configManger;
        this.supervisor = configManger.getSupervisor();
    }
    
    @Override
    public void handle(final HttpRequest request, final HttpResponse response,
            final HttpContext context) throws HttpException, IOException {

        String method = request.getRequestLine().getMethod()
                .toUpperCase(Locale.ENGLISH);

        LOG.debug("Frontend: Handling Search; Line = " + request.getRequestLine());
        if (method.equals("GET")) {//TODO how to post
            final String target = request.getRequestLine().getUri();
            Pattern p = Pattern.compile("/paaslogout\\?app=(.*)&ids=(.*)&ips=(.*)$");
            Matcher m = p.matcher(target);
            if (m.find()) {
                String app = m.group(1);
                String instancesString = m.group(2);
                String IPsString = m.group(3);
                String[] ids = instancesString.split(",");
                String[] ips = IPsString.split(",");
                if (ids.length == 0 || ips.length == 0 || ids.length != ips.length) {
                    response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                    return;
                }
                LOG.debug("Handle paas logout request, app: " + app + " instances: " + Arrays.toString(ids));
                final HttpResult Content = getContent(app, ids, ips);
                EntityTemplate body = new EntityTemplate(new ContentProducer() {
                    public void writeTo(final OutputStream outstream)
                            throws IOException {
                        OutputStreamWriter writer = new OutputStreamWriter(
                                outstream, "UTF-8");
                        writer.write(Content.code);
                        writer.write("|");
                        writer.write(Content.msg);
                        writer.flush();
                    }
                });
                body.setContentType("application/json; charset=UTF-8");
                
                response.setStatusCode(HttpStatus.SC_OK);
                response.setEntity(body);
            } else {
                response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
            }
        } else {
            throw new MethodNotSupportedException(method
                    + " method not supported\n");
        }
    }
    
    /**
     * This method is synchronized, it will block until all agent return or timeout.
     * @param app cmdb app name
     * @param args String[] args[0] stand for instanceIds, args[1] stand for associated IPs
     */
    @Override
    public HttpResult getContent(String app, String[]... args) {
        Set<String> topicSet = configManager.getTopicsByCmdb(app);
        if (topicSet == null || topicSet.size() == 0) {
            return new HttpResult(HttpResult.NONEED, "It contains no mapping for the cmdbapp " + app);
        } else {
            LOG.info(Arrays.toString(args[0]) + " login, it belong to " + app);
        }
        String[] ids = args[0];
        String[] ips = args[1];
        Map<String, List<String>> hostIds = extractIdMapShuffleByHost(ids, ips);
        
        Map<SimpleConnection, Message> toBeQuit = new HashMap<SimpleConnection, Message>();
        Map<SimpleConnection, Message> toBeClean = new HashMap<SimpleConnection, Message>();
        for(Map.Entry<String, List<String>> entry : hostIds.entrySet()) {
            String eachHost = entry.getKey();
            List<String> idsInTheSameHost = entry.getValue();
            SimpleConnection agent = supervisor.getConnectionByHostname(eachHost);
            if (agent == null) {
                LOG.info("Can not find any agents connected by " + eachHost);
                return new HttpResult(HttpResult.FAILURE, "Can not find any agents connected by " + eachHost);
            }
            List<InstanceGroup> quits = new ArrayList<InstanceGroup>();
            List<InstanceGroup> cleans = new ArrayList<InstanceGroup>();
            for (String topic : topicSet) {
                // filter the stream already active instance
                filterIsInactive(topic, eachHost, idsInTheSameHost, supervisor);
                InstanceGroup quit = PBwrap.wrapInstanceGroup(topic, idsInTheSameHost);
                InstanceGroup clean = PBwrap.wrapInstanceGroup(topic, idsInTheSameHost);
                quits.add(quit);
                cleans.add(clean);
            }
            Message quitMessage = PBwrap.wrapQuit(quits);
            Message cleanMessage = PBwrap.wrapClean(cleans);
            toBeQuit.put(agent, quitMessage);
            toBeClean.put(agent, cleanMessage);
        }
        
        return sendAndReceive(toBeQuit, toBeClean, supervisor);
    }
    
    private HttpResult sendAndReceive(
            Map<SimpleConnection, Message> toBeQuit,
            Map<SimpleConnection, Message> toBeClean,
            Supervisor supervisor) {
        HttpResult result = sendAndReceiveForQuit(toBeQuit, supervisor);
        if (result.code == HttpResult.SUCCESS) {
            return sendAndReceiveForClean(toBeClean, supervisor);
        } else {
            return result;
        }
    }
    
    private HttpResult sendAndReceiveForQuit(Map<SimpleConnection, Message> toBeQuit, Supervisor supervisor) {
        long currentTime = System.currentTimeMillis();
        long timeout = currentTime + TIMEOUT;
        while (currentTime < timeout) {
            if (checkStreamsEmpty(toBeQuit, supervisor)) {
                return new HttpResult(HttpResult.SUCCESS, "");
            }
            supervisor.cachedSend(toBeQuit);
            currentTime += CHECK_PERIOD;
            try {
                Thread.sleep(CHECK_PERIOD);
            } catch (InterruptedException e) {
                return new HttpResult(HttpResult.FAILURE, "Thread interrupted");
            }
        }
        return new HttpResult(HttpResult.FAILURE, "timeout");
    }
    
    private HttpResult sendAndReceiveForClean(Map<SimpleConnection, Message> toBeClean, Supervisor supervisor) {
        long currentTime = System.currentTimeMillis();
        long timeout = currentTime + TIMEOUT;
        while (currentTime < timeout) {
            if (checkStreamsClean(toBeClean, supervisor)) {
                return new HttpResult(HttpResult.SUCCESS, "");
            }
            supervisor.cachedSend(toBeClean);
            currentTime += CHECK_PERIOD;
            try {
                Thread.sleep(CHECK_PERIOD);
            } catch (InterruptedException e) {
                return new HttpResult(HttpResult.FAILURE, "Thread interrupted");
            }
        }
        return new HttpResult(HttpResult.FAILURE, "timeout");
    }

    private boolean checkStreamsEmpty(Map<SimpleConnection, Message> toBeSend, Supervisor supervisor) {
        // loop for every agent server
        for (Map.Entry<SimpleConnection, Message> entry : toBeSend.entrySet()) {
            String agentServer = entry.getKey().getHost();
            // loop for every topic in an agent server
            for (LxcConfRes lxcConfRes : entry.getValue().getConfRes().getLxcConfResList()) {
                String topic = lxcConfRes.getTopic();
                List<String> idsInTheSameHost = lxcConfRes.getInstanceIdsList();
                // loop for every instance with the same topic in the same agent server
                for (String id : idsInTheSameHost) {
                    if (!supervisor.isEmptyStream(topic, Util.getSourceIdentify(agentServer, id))) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
    
    private boolean checkStreamsClean(Map<SimpleConnection, Message> toBeSend, Supervisor supervisor) {
        for (Map.Entry<SimpleConnection, Message> entry : toBeSend.entrySet()) {
            String agentServer = entry.getKey().getHost();
            for (LxcConfRes lxcConfRes : entry.getValue().getConfRes().getLxcConfResList()) {
                String topic = lxcConfRes.getTopic();
                List<String> idsInTheSameHost = lxcConfRes.getInstanceIdsList();
                for (String id : idsInTheSameHost) {
                    if (!supervisor.isCleanStream(topic, Util.getSourceIdentify(agentServer, id))) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}

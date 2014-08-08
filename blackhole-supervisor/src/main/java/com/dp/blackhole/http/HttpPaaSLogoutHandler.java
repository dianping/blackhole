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
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.QuitAndCleanPB.InstanceGroup;
import com.dp.blackhole.supervisor.ConfigManager;
import com.dp.blackhole.supervisor.Supervisor;

public class HttpPaaSLogoutHandler extends HttpAbstractHandler implements HttpRequestHandler {
    private static Logger LOG = Logger.getLogger(HttpPaaSLogoutHandler.class);
    private ConfigManager configManager;
    private Supervisor supervisor;
    
    public HttpPaaSLogoutHandler(ConfigManager configManger, HttpClientSingle httpClient) {
        this.configManager = configManger;
        this.supervisor = configManger.getSupervisor();
    }
    
    @Override
    public void handle(final HttpRequest request, final HttpResponse response,
            final HttpContext context) throws HttpException, IOException {

        String method = request.getRequestLine().getMethod()
                .toUpperCase(Locale.ENGLISH);

        LOG.debug("Frontend: Handling Search; Line = " + request.getRequestLine());
        if (method.equals("GET")) {
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
        
        Map<String, Message> toBeQuit = new HashMap<String, Message>();
        Map<String, Message> toBeClean = new HashMap<String, Message>();
        for(Map.Entry<String, List<String>> entry : hostIds.entrySet()) {
            String eachHost = entry.getKey();
            List<String> idsInTheSameHost = entry.getValue();
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
            toBeQuit.put(eachHost, quitMessage);
            toBeClean.put(eachHost, cleanMessage);
        }
        
        return sendAndReceive(toBeQuit, toBeClean, supervisor);
    }
    
    private HttpResult sendAndReceive(
            Map<String, Message> toBeQuit,
            Map<String, Message> toBeClean,
            Supervisor supervisor) {
        HttpResult result = sendAndReceiveForQuit(toBeQuit, supervisor);
        if (result.code == HttpResult.SUCCESS) {
            return sendAndReceiveForClean(toBeClean, supervisor);
        } else {
            return result;
        }
    }
    
    private HttpResult sendAndReceiveForQuit(Map<String, Message> toBeQuit, Supervisor supervisor) {
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
    
    private HttpResult sendAndReceiveForClean(Map<String, Message> toBeClean, Supervisor supervisor) {
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

    private boolean checkStreamsEmpty(Map<String, Message> toBeSend, Supervisor supervisor) {
        // loop for every agent server
        for (Map.Entry<String, Message> entry : toBeSend.entrySet()) {
            String agentServer = entry.getKey();
            // loop for every topic in an agent server
            for (InstanceGroup instanceGroup : entry.getValue().getQuit().getInstanceGroupList()) {
                String topic = instanceGroup.getTopic();
                List<String> idsInTheSameHost = instanceGroup.getInstanceIdsList();
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
    
    private boolean checkStreamsClean(Map<String, Message> toBeSend, Supervisor supervisor) {
        for (Map.Entry<String, Message> entry : toBeSend.entrySet()) {
            String agentServer = entry.getKey();
            for (InstanceGroup instanceGroup : entry.getValue().getClean().getInstanceGroupList()) {
                String topic = instanceGroup.getTopic();
                List<String> idsInTheSameHost = instanceGroup.getInstanceIdsList();
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

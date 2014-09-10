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
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.conf.Context;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes.LxcConfRes;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.supervisor.ConfigManager;
import com.dp.blackhole.supervisor.Supervisor;

public class HttpPaaSLoginHandler extends HttpAbstractHandler implements HttpRequestHandler {
    private static Logger LOG = Logger.getLogger(HttpPaaSLoginHandler.class);
    private ConfigManager configManager;
    private Supervisor supervisor;
    
    public HttpPaaSLoginHandler(ConfigManager configManger, HttpClientSingle httpClient) {
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
            Pattern p = Pattern.compile("/paaslogin\\?app=(.*)&ids=(.*)&ips=(.*)$");
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
                LOG.debug("Handle paas login request, app: " + app + " instances: " + Arrays.toString(ids));
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
        
        Map<String, Message> toBeSend = new HashMap<String, Message>();
        for(Map.Entry<String, List<String>> entry : hostIds.entrySet()) {
            String eachHost = entry.getKey();
            List<String> idsInTheSameHost = entry.getValue();
            List<LxcConfRes> lxcConfResList = new ArrayList<LxcConfRes>();
            for (String topic : topicSet) {
                if (inBlacklist(topic)) {
                    continue;
                }
                // filter the stream already active instance
                filterIsActive(topic, eachHost, idsInTheSameHost, supervisor);
                
                Context context = ConfigKeeper.configMap.get(topic);
                if (context == null) {
                    LOG.error("Can not get topic: " + topic + " from configMap");
                    return new HttpResult(HttpResult.FAILURE, "Can not get topic: " + topic + " from configMap");
                }
                String period = context.getString(ParamsKey.TopicConf.ROLL_PERIOD);
                String maxLineSize = context.getString(ParamsKey.TopicConf.MAX_LINE_SIZE);
                String watchFile = context.getString(ParamsKey.TopicConf.WATCH_FILE);
                LxcConfRes lxcConfRes = PBwrap.wrapLxcConfRes(topic, watchFile, period, maxLineSize, idsInTheSameHost);
                lxcConfResList.add(lxcConfRes);
            }
            Message message = PBwrap.wrapConfRes(null, lxcConfResList);
            toBeSend.put(eachHost, message);
        }
        
        return sendAndReceive(toBeSend, supervisor);
    }
    
    private HttpResult sendAndReceive(Map<String, Message> toBeSend, Supervisor supervisor) {
        long currentTime = System.currentTimeMillis();
        long timeout = currentTime + TIMEOUT;
        while (currentTime < timeout) {
            if (checkStreamsActive(toBeSend, supervisor)) {
                LOG.info("all stream active, instances login succcss.");
                return new HttpResult(HttpResult.SUCCESS, "");
            }
            supervisor.cachedSend(toBeSend);
            currentTime += CHECK_PERIOD;
            try {
                Thread.sleep(CHECK_PERIOD);
            } catch (InterruptedException e) {
                return new HttpResult(HttpResult.FAILURE, "Thread interrupted");
            }
        }
        return new HttpResult(HttpResult.FAILURE, "timeout");
    }
    
    private boolean checkStreamsActive(Map<String, Message> toBeSend, Supervisor supervisor) {
        // loop for every agent server
        for (Map.Entry<String, Message> entry : toBeSend.entrySet()) {
            String agentServer = entry.getKey();
            // loop for every topic in an agent server
            for (LxcConfRes lxcConfRes : entry.getValue().getConfRes().getLxcConfResList()) {
                String topic = lxcConfRes.getTopic();
                List<String> idsInTheSameHost = lxcConfRes.getInstanceIdsList();
                // loop for every instance with the same topic in the same agent server
                for (String id : idsInTheSameHost) {
                    if (!supervisor.isActiveStream(topic, Util.getSourceIdentify(agentServer, id))) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}

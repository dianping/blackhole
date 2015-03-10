package com.dp.blackhole.http;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.QuitAndCleanPB.InstanceGroup;
import com.dp.blackhole.supervisor.ConfigManager;
import com.dp.blackhole.supervisor.Supervisor;

public class HttpScaleInHandler extends HttpAbstractHandler implements HttpRequestHandler {
    private static Logger LOG = Logger.getLogger(HttpScaleInHandler.class);
    private ConfigManager configManager;
    private Supervisor supervisor;
    
    public HttpScaleInHandler(ConfigManager configManager) {
        this.configManager = configManager;
        this.supervisor = configManager.getSupervisor();
    }
    
    @Override
    public void handle(final HttpRequest request, final HttpResponse response,
            final HttpContext context) throws HttpException, IOException {

        String method = request.getRequestLine().getMethod()
                .toUpperCase(Locale.ENGLISH);

        LOG.debug("Frontend: Handling scale-in; Line = " + request.getRequestLine());
        if (method.equals("GET")) {
            final String target = request.getRequestLine().getUri();
            Pattern p = Pattern.compile("/scalein\\?app=(.*)&hosts=(.*)$");
            Matcher m = p.matcher(target);
            if (m.find()) {
                String app = m.group(1);
                String hostnameString = m.group(2);
                String[] hostnames = hostnameString.split(",");
                if (hostnames.length == 0) {
                    response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                    return;
                }
                LOG.debug("Handle scale-in request, app: " + app + " host: " + Arrays.toString(hostnames));
                final HttpResult Content = getContent(app, hostnames);
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
    
    @Override
    public HttpResult getContent(String app, String[]... args) {
        Set<String> topicList = configManager.getTopicsByCmdb(app);
        if (topicList == null || topicList.size() == 0) {
            return new HttpResult(HttpResult.NONEED, "It contains no mapping for the cmdbapp " + app);
        }
        for (final String topic : topicList) {
            String watchKey = ParamsKey.LionNode.HOSTS_PREFIX + topic;
            try {
                configManager.updateLionList(watchKey, ParamsKey.LionNode.OP_SCALEIN, args[0]);
            } catch (HttpException e) {
                return new HttpResult(HttpResult.FAILURE, e.getMessage());
            }
            for (int i = 0; i < args[0].length; i++) {
                final String eachHost = args[0][i];
                httpWorkerPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        Set<String> noIds = new HashSet<String>();
                        noIds.add("");
                        InstanceGroup quit = PBwrap.wrapInstanceGroup(topic, noIds);
                        InstanceGroup clean = PBwrap.wrapInstanceGroup(topic, noIds);
                        List<InstanceGroup> quits = new ArrayList<InstanceGroup>();
                        List<InstanceGroup> cleans = new ArrayList<InstanceGroup>();
                        quits.add(quit);
                        cleans.add(clean);
                        Message quitMessage = PBwrap.wrapQuit(quits);
                        Message cleanMessage = PBwrap.wrapClean(cleans);
                        sendAndReceive(topic, eachHost, quitMessage, cleanMessage);
                    }
                });
            }
        }
        return new HttpResult(HttpResult.SUCCESS, "");
    }
    
    public HttpResult sendAndReceive(String topic, String host, Message toBeQuit, Message toBeClean) {
        HttpResult result = sendAndReceiveForQuit(topic, host, toBeQuit);
        if (result.code == HttpResult.SUCCESS) {
            cleanAgentResources(topic, host, toBeClean);
        }
        return result;
    }
    
    private HttpResult sendAndReceiveForQuit(String topic, String host, Message toBeQuit) {
        long currentTime = Util.getTS();
        long timeout = currentTime + TIMEOUT;
        while (currentTime < timeout) {
            supervisor.cachedSend(host, toBeQuit);
            if (checkStreamEmpty(topic, host)) {
                return new HttpResult(HttpResult.SUCCESS, "");
            }
            currentTime += CHECK_PERIOD;
            try {
                Thread.sleep(CHECK_PERIOD);
            } catch (InterruptedException e) {
                return new HttpResult(HttpResult.FAILURE, "Thread interrupted");
            }
        }
        return new HttpResult(HttpResult.FAILURE, "timeout");
    }
    
    private void cleanAgentResources(String topic, String host, Message toBeClean) {
        long currentTime = Util.getTS();
        long timeout = currentTime + TIMEOUT;
        supervisor.cachedSend(host, toBeClean);
        while (currentTime < timeout) {
            if (checkStreamClean(topic, host)) {
                LOG.info(topic + "-" + host + " stream empty, logout succcss.");
                break;
            }
            currentTime += CHECK_PERIOD;
            try {
                Thread.sleep(CHECK_PERIOD);
            } catch (InterruptedException e) {
            }
        }
    }

    private boolean checkStreamEmpty(String topic, String source) {
        return supervisor.isEmptyStream(topic, source);
    }
    
    private boolean checkStreamClean(String topic, String source) {
        return supervisor.isCleanStream(topic, source);
    }
}
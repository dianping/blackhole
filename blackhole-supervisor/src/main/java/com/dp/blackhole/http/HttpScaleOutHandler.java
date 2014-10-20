package com.dp.blackhole.http;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
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

import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.supervisor.ConfigManager;

public class HttpScaleOutHandler extends HttpAbstractHandler implements HttpRequestHandler {
    private static Logger LOG = Logger.getLogger(HttpScaleOutHandler.class);
    private ConfigManager configManager;
    
    public HttpScaleOutHandler(ConfigManager configManager) {
        this.configManager = configManager;
    }
    
    @Override
    public void handle(final HttpRequest request, final HttpResponse response,
            final HttpContext context) throws HttpException, IOException {

        String method = request.getRequestLine().getMethod()
                .toUpperCase(Locale.ENGLISH);

        LOG.debug("Frontend: Handling Search; Line = " + request.getRequestLine());
        if (method.equals("GET")) {
            final String target = request.getRequestLine().getUri();
            Pattern p = Pattern.compile("/scaleout\\?app=(.*)&hosts=(.*)$");
            Matcher m = p.matcher(target);
            if (m.find()) {
                String app = m.group(1);
                String hostnameString = m.group(2);
                String[] hostnames = hostnameString.split(",");
                if (hostnames.length == 0) {
                    response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                    return;
                }
                LOG.debug("Handle scaleout request, app: " + app + " host: " + Arrays.toString(hostnames));
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
    public HttpResult getContent(String app, String[]...args) {
        Set<String> topicList = configManager.getTopicsByCmdb(app);
        if (topicList == null || topicList.size() == 0) {
            return new HttpResult(HttpResult.NONEED, "It contains no mapping for the cmdbapp " + app);
        }
        for (String topic : topicList) {
            String watchKey = ParamsKey.LionNode.HOSTS_PREFIX + topic;
            try {
                configManager.updateLionList(watchKey, ParamsKey.LionNode.OP_SCALEOUT, args[0]);
            } catch (HttpException e) {
                return new HttpResult(HttpResult.FAILURE, e.getMessage());
            }
        }
        return new HttpResult(HttpResult.SUCCESS, "");
    }
}
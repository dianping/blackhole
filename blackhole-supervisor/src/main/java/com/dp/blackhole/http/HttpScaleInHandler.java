package com.dp.blackhole.http;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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
import com.dp.blackhole.common.Util;
import com.dp.blackhole.supervisor.ConfigManager;

public class HttpScaleInHandler extends HttpAbstractHandler implements HttpRequestHandler {
    private static Logger LOG = Logger.getLogger(HttpScaleInHandler.class);
    private ConfigManager configManager;
    private HttpClientSingle httpClient;
    
    public HttpScaleInHandler(ConfigManager configManager, HttpClientSingle httpClient) {
        this.configManager = configManager;
        this.httpClient = httpClient;
    }
    
    @Override
    public void handle(final HttpRequest request, final HttpResponse response,
            final HttpContext context) throws HttpException, IOException {

        String method = request.getRequestLine().getMethod()
                .toUpperCase(Locale.ENGLISH);

        LOG.debug("Frontend: Handling contract; Line = " + request.getRequestLine());
        if (method.equals("GET")) {//TODO how to post
            final String target = request.getRequestLine().getUri();
            Pattern p = Pattern.compile("/contract\\?app=(.*)&hosts=(.*)$");
            Matcher m = p.matcher(target);
            if (m.find()) {
                String app = m.group(1);
                String hostnameString = m.group(2);
                String[] hostnames = hostnameString.split(",");
                if (hostnames.length == 0) {
                    response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                    return;
                }
                LOG.debug("Handle contract request, app: " + app + " host: " + Arrays.toString(hostnames));
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
        for (String topic : topicList) {
            //get string of old hosts of the app
            String watchKey = ParamsKey.LionNode.HOSTS_PREFIX + topic;
            String url = configManager.generateGetURL(watchKey);
            String response = httpClient.getResponseText(url);
            if (response == null) {
                return new HttpResult(HttpResult.FAILURE, "IO exception was thrown when handle url ." + url);
            } else if (response.startsWith("1|")) {
                return new HttpResult(HttpResult.FAILURE, response.substring(2));
            } else if (response.equals("<null>")) {
                return new HttpResult(HttpResult.FAILURE, "No configration in lion for key=" + watchKey);
            } else if (response.length() == 0) {
                return new HttpResult(HttpResult.FAILURE, "Invalid response");
            }
            String[] oldHosts = Util.getStringListOfLionValue(response);

            ArrayList<String> newHostList = new ArrayList<String>();
            //change it (sub the given hostname)
            if (oldHosts == null) {
                LOG.error("Faild to contract hosts cause there is no host in lion for topic " + topic);
                continue;
            } else {
                
                Set<String> contractSet = new HashSet<String>();
                for (String contractHost : args[0]) {
                    contractSet.add(contractHost);
                }
                for (String old : oldHosts) {
                    if (!contractSet.contains(old)) {
                        newHostList.add(old);
                    }
                }
            }
            String[] newHosts = new String[newHostList.size()];
            String newHostsLionString = Util.getLionValueOfStringList(newHostList.toArray(newHosts));
            url = configManager.generateSetURL(watchKey, newHostsLionString);
            response = httpClient.getResponseText(url);
            if (response == null) {
                return new HttpResult(HttpResult.FAILURE, "IO exception was thrown when handle url ." + url);
            } else if (response.startsWith("1|")) {
                return new HttpResult(HttpResult.FAILURE, "No configration in lion for key=" + watchKey);
            } else if (response.startsWith("0")) {
            } else {
                LOG.error("Unkown response.");
                return new HttpResult(HttpResult.FAILURE, "Unkown response.");
            }
        }
        return new HttpResult(HttpResult.SUCCESS, "");
    }
}
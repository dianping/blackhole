package com.dp.blackhole.scaleout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import com.dp.blackhole.common.Util;
import com.dp.blackhole.supervisor.LionConfChange;

public class HttpScalaoutHandler extends HttpAbstractHandler implements HttpRequestHandler {
    private static Logger LOG = Logger.getLogger(HttpScalaoutHandler.class);
    private LionConfChange lionConfChange;
    private HttpClientSingle httpClient;
    
    public HttpScalaoutHandler(LionConfChange lionConfChange, HttpClientSingle httpClient) {
        this.lionConfChange = lionConfChange;
        this.httpClient = httpClient;
    }
    
    @Override
    public void handle(final HttpRequest request, final HttpResponse response,
            final HttpContext context) throws HttpException, IOException {

        String method = request.getRequestLine().getMethod()
                .toUpperCase(Locale.ENGLISH);

        LOG.debug("Frontend: Handling Search; Line = " + request.getRequestLine());
        if (method.equals("GET")) {//TODO how to post
            final String target = request.getRequestLine().getUri();
            Pattern p = Pattern.compile("/scaleout\\?app=(.*)&host=(.*)$");
            Matcher m = p.matcher(target);
            if (m.find()) {
                String app = m.group(1);
                String hostname = m.group(2);
                LOG.debug("Handle scalaout request, app: " + app + " host: " + hostname);
                final HttpResult Content = getContent(app, hostname);
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
    public HttpResult getContent(String cmdbApp, String hostname) {
        Set<String> topicList = lionConfChange.getAppNamesByCmdb(cmdbApp);
        if (topicList == null || topicList.size() == 0) {
            return new HttpResult(HttpResult.NONEED, "It contains no mapping for the cmdbapp " + cmdbApp);
        }
        for (String topic : topicList) {
            //get string of old hosts of the app
            String watchKey = ParamsKey.LionNode.APP_HOSTS_PREFIX + topic;
            String url = lionConfChange.generateGetURL(watchKey);
            String response = getResponseText(url);
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

            String[] newHosts = null;
            //change it (add the given hostname)
            if (oldHosts == null) {
                newHosts =  new String[]{hostname};
            } else {
                newHosts = Arrays.copyOf(oldHosts, oldHosts.length + 1);
                newHosts[newHosts.length - 1] = hostname;
            }

            String newHostsLionString = Util.getLionValueOfStringList(newHosts);
            url = lionConfChange.generateSetURL(watchKey, newHostsLionString);
            response = getResponseText(url);
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
    
    public String getResponseText(String url) {
        LOG.debug("http client access url: " + url);
        StringBuilder responseBuilder = new StringBuilder();
        BufferedReader bufferedReader = null;
        InputStream is = null;
        try {
            is = httpClient.getResource(url);
        } catch (IOException e) {
            LOG.error("Can not get http response. " + e.getMessage());
            return null;
        }
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(is, "UTF-8"), 8 * 1024);
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                responseBuilder.append(line + "\n");
            }
            if (responseBuilder.length() != 0) {
                responseBuilder.deleteCharAt(responseBuilder.length() - 1);
            }
        } catch (IOException e) {
            LOG.error("Oops, got an exception in reading http response content." + e.getMessage());
            return null;
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
            }
        }
        return responseBuilder.toString();
    }
}
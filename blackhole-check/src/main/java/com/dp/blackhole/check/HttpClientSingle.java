package com.dp.blackhole.check;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.impl.conn.SchemeRegistryFactory;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

public class HttpClientSingle {
    private static Logger LOG = Logger.getLogger(HttpClientSingle.class);
    private final DefaultHttpClient httpClient;
    
    public HttpClientSingle() {
        this(15000, 15000);
    }
    
    public HttpClientSingle(int connectionTimeout, int socketTimeout) {
        PoolingClientConnectionManager conMan = new PoolingClientConnectionManager(SchemeRegistryFactory.createDefault() );
        conMan.setMaxTotal(200);
        conMan.setDefaultMaxPerRoute(200);
        httpClient = new DefaultHttpClient(conMan);
        HttpParams params = httpClient.getParams();
        HttpConnectionParams.setConnectionTimeout(params, connectionTimeout);
        HttpConnectionParams.setSoTimeout(params, socketTimeout);
    }
    
    private synchronized HttpEntity getResource(String uri) throws IOException {
        HttpGet method = new HttpGet(uri);
        HttpResponse httpResponse = this.httpClient.execute(method);
        int statusCode = httpResponse.getStatusLine().getStatusCode();
        if (HttpStatus.SC_OK == statusCode) {
            LOG.trace("200 OK request");
            return httpResponse.getEntity();
        } else {
            throw new IOException("Something went wrong, statusCode is " + statusCode);
        }
    }
    
    public String getResponseText(String uri) {
        LOG.trace("http client access uri: " + uri);
        try {
            return EntityUtils.toString(getResource(uri), "utf-8");
        } catch (IOException e) {
            LOG.error("Oops, got an exception in reading http response content." + e.getMessage());
            return null;
        }
    }
}

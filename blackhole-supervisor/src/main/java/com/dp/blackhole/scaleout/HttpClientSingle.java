package com.dp.blackhole.scaleout;

import java.io.IOException;
import java.io.InputStream;

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
    
    public HttpClientSingle(int connectionTimeout, int socketTimeout) {
        PoolingClientConnectionManager conMan = new PoolingClientConnectionManager(SchemeRegistryFactory.createDefault() );
        conMan.setMaxTotal(200);
        conMan.setDefaultMaxPerRoute(200);
        httpClient = new DefaultHttpClient(conMan);
        HttpParams params = httpClient.getParams();
        HttpConnectionParams.setConnectionTimeout(params, connectionTimeout);
        HttpConnectionParams.setSoTimeout(params, socketTimeout);
    }
    
    public InputStream getResource(String uri) throws IOException {
        HttpGet method = new HttpGet(uri);
        HttpResponse httpResponse = this.httpClient.execute(method);
        int statusCode = httpResponse.getStatusLine().getStatusCode();
        InputStream is = null;
        if (HttpStatus.SC_OK == statusCode) {
            LOG.debug("200 OK request");
            is = httpResponse.getEntity().getContent();
            EntityUtils.consume(httpResponse.getEntity());
        } else {
            EntityUtils.consume(httpResponse.getEntity());
            throw new IOException("Something went wrong, statusCode is " + statusCode);
        }
        return is;
    }
}

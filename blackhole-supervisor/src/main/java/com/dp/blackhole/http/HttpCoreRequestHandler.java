package com.dp.blackhole.http;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpException;
import org.apache.http.HttpServerConnection;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpService;

public class HttpCoreRequestHandler implements Runnable {
    private static final Log LOG = LogFactory.getLog(HttpCoreRequestHandler.class);
    private final HttpService httpservice;
    private final HttpServerConnection conn;
    
    public HttpCoreRequestHandler(
            final HttpService httpservice, 
            final HttpServerConnection conn) {
        this.httpservice = httpservice;
        this.conn = conn;
    }

    public void run() {
        HttpContext context = new BasicHttpContext(null);
        try {
            while (!Thread.interrupted() && this.conn.isOpen()) {
                this.httpservice.handleRequest(this.conn, context);
            }
        } catch (ConnectionClosedException e) {
            LOG.error("Client closed connection", e);
        } catch (IOException e) {
            LOG.error("I/O error: ", e);
        } catch (HttpException e) {
            LOG.error("Unrecoverable HTTP protocol violation: ", e);
        } finally {
            try {
                this.conn.shutdown();
            } catch (IOException ignore) {}
        }
    }
}

package com.dp.blackhole.http;

import java.io.IOException;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.log4j.Logger;

public class HttpFallbackHandler implements HttpRequestHandler {

    private static Logger LOG = Logger.getLogger(HttpFallbackHandler.class);

    public void handle(final HttpRequest request, final HttpResponse response,
            final HttpContext context) throws HttpException, IOException {

        LOG.info("Page not found");
        response.setStatusCode(HttpStatus.SC_NOT_FOUND);
    }

}
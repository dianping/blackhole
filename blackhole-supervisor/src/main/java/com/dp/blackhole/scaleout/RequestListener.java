package com.dp.blackhole.scaleout;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.HttpResponseInterceptor;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.DefaultHttpServerConnection;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpRequestHandlerRegistry;
import org.apache.http.protocol.HttpService;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;
import org.apache.log4j.Logger;

import com.dp.blackhole.supervisor.LionConfChange;

public class RequestListener extends Thread {
    private static Logger LOG = Logger.getLogger(RequestListener.class);
    private final ServerSocket serversocket;
    private final HttpParams params; 
    private final HttpService httpService;
    private boolean running;
    private ExecutorService handlerPool = Executors.newCachedThreadPool();
    
    public RequestListener(int port, LionConfChange lionConfChange, HttpClientSingle cmdbHttpClient) throws IOException {
        this.serversocket = new ServerSocket(port);
        this.params = new SyncBasicHttpParams();
        this.params
            .setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 120000)
            .setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024)
            .setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
            .setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
            .setParameter(CoreProtocolPNames.ORIGIN_SERVER, "HttpComponents/1.1");

        // Set up the HTTP protocol processor
        HttpProcessor httpproc = new ImmutableHttpProcessor(new HttpResponseInterceptor[] {
                new ResponseDate(),
                new ResponseServer(),
                new ResponseContent(),
                new ResponseConnControl()
        });
        
        // Set up request handlers
        HttpRequestHandlerRegistry reqistry = new HttpRequestHandlerRegistry();
        reqistry.register("/scaleout*", new HttpScaleoutHandler(lionConfChange, cmdbHttpClient));
        reqistry.register("/contract*", new HttpContractHandler(lionConfChange, cmdbHttpClient));
        reqistry.register("*", new HttpFallbackHandler());
        
        // Set up the HTTP service
        this.httpService = new HttpService(
                httpproc, 
                new DefaultConnectionReuseStrategy(), 
                new DefaultHttpResponseFactory(),
                reqistry,
                this.params);
    }
    
    public void run() {
        LOG.info("Listening on port " + this.serversocket.getLocalPort());
        this.running = true;
        while (running && !Thread.interrupted()) {
            try {
                // Set up HTTP connection
                Socket socket = this.serversocket.accept();
                DefaultHttpServerConnection conn = new DefaultHttpServerConnection();
                LOG.info("Incoming connection from " + socket.getInetAddress());
                conn.bind(socket, this.params);

                // Start worker thread
                HttpCoreRequestHandler handler = new HttpCoreRequestHandler(this.httpService, conn);
                handlerPool.execute(handler);
            } catch (IOException e) {
                running = false;
                try {
                    if (serversocket != null) {
                        serversocket.close();
                    }
                } catch (IOException e1) {
                }
            }
        }
    }
}
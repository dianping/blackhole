package com.dp.blackhole.consumer;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.consumer.Fetcher;

public class SimpleOperation implements Closeable {
    
    private final Log logger = LogFactory.getLog(SimpleOperation.class);

    private final String host;

    private final int port;

    private final int soTimeout;

    private final int bufferSize;

    ////////////////////////////////
    private SocketChannel channel = null;

    private final Object lock = new Object();

    public SimpleOperation(String host, int port) {
        this(host, port, 30 * 1000, 64 * 1024);
    }

    public SimpleOperation(String host, int port, int soTimeout, int bufferSize) {
        super();
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
    }

    private SocketChannel connect() throws IOException {
        close();
        InetSocketAddress address = new InetSocketAddress(host, port);
        SocketChannel ch = SocketChannel.open();
        logger.debug("Connected to " + address + " for fetching");
        ch.configureBlocking(true);
        ch.socket().setReceiveBufferSize(bufferSize);
        ch.socket().setSoTimeout(soTimeout);
        ch.socket().setKeepAlive(true);
        ch.socket().setTcpNoDelay(true);
        ch.connect(address);
        return ch;
    }

    public void close() {
        synchronized (lock) {
            if (channel != null) {
                close(channel);
                channel = null;
            }
        }
    }

    private void close(SocketChannel socketChannel) {
        logger.debug("Disconnecting consumer from " + channel.socket().getRemoteSocketAddress());
        Util.closeQuietly(socketChannel);
        Util.closeQuietly(socketChannel.socket());
    }

    private void getOrMakeConnection() throws IOException {
        if (channel == null) {
            channel = connect();
        }
    }

    public static interface Command<T> {

        T run() throws IOException;
    }

    private class SimpleCommand implements Command<KV<Receive, ErrorMapping>> {

        private Request request;

        public SimpleCommand(Request request) {
            this.request = request;
        }

        public KV<Receive, ErrorMapping> run() throws IOException {
            synchronized (lock) {
                getOrMakeConnection();
                try {
                    sendRequest(request);
                    return getResponse();
                } catch (IOException e) {
                    logger.info("Reconnect in fetch request due to socket error:", e);
                    //retry once
                    try {
                        channel = connect();
                        sendRequest(request);
                        return getResponse();
                    } catch (IOException e2) {
                        throw e2;
                    }
                }
                //
            }
        }
    }

    public KV<Receive, ErrorMapping> send(Request request) throws IOException {
        return new SimpleCommand(request).run();
    }

    public ByteBufferMessageSet fetch(FetchRequest request) throws IOException {
        KV<Receive, ErrorMapping> response = send(request);
        return new ByteBufferMessageSet(response.k.buffer(), request.offset, response.v);
    }

    protected void sendRequest(Request request) throws IOException {
        new BoundedByteBufferSend(request).writeCompletely(channel);
    }

    protected KV<Receive, ErrorMapping> getResponse() throws IOException {
        BoundedByteBufferReceive response = new BoundedByteBufferReceive();
        response.readCompletely(channel);
        return new KV<Receive, ErrorMapping>(response, ErrorMapping.valueOf(response.buffer().getShort()));
    }

}

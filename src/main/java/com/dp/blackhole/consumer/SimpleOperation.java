package com.dp.blackhole.consumer;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.collectornode.persistent.protocol.DataMessageTypeFactory;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.network.TypedFactory;
import com.dp.blackhole.network.TypedWrappable;

public class SimpleOperation implements Closeable {
    
    private final Log logger = LogFactory.getLog(SimpleOperation.class);

    private final String host;

    private final int port;

    private final int soTimeout;

    private final int bufferSize;

    private final TypedFactory factory;
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
        factory = new DataMessageTypeFactory();
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
        if (socketChannel != null) {
            try {
                socketChannel.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (socketChannel.socket() != null) {
            try {
                socketChannel.socket().close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private void getOrMakeConnection() throws IOException {
        if (channel == null) {
            channel = connect();
        }
    }

    private class SimpleCommand {

        private TypedWrappable request;

        public SimpleCommand(TypedWrappable request) {
            this.request = request;
        }

        public TypedWrappable run() throws IOException {
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

    public TypedWrappable send(TypedWrappable request) throws IOException {
        return new SimpleCommand(request).run();
    }
    
//    public ByteBufferMessageSet fetch(FetchRequest request) throws IOException {
//        KV<MultiFetchReply, ErrorMapping> response = send(request);
//        return new ByteBufferMessageSet(response.k.buffer(), request.offset, response.v);
//    }

    protected void sendRequest(TypedWrappable request) throws IOException {
        TransferWrap sent = new TransferWrap(request);
        while (!sent.complete()) {
            sent.write(channel);
        }
    }

    protected TypedWrappable getResponse() throws IOException {
        TransferWrap receive = new TransferWrap(factory);
        while (!receive.complete()) {
            receive.read(channel);
        }
        TypedWrappable response = receive.unwrap();
        return response;//new KV<TypedWrappable, ErrorMapping>(response, ErrorMapping.valueOf(response.buffer().getShort()));
    }
}

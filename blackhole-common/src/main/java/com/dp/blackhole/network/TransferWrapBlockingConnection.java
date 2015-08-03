package com.dp.blackhole.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;

public class TransferWrapBlockingConnection implements BlockingConnection<TransferWrap> {

    public static class TransferWrapBlockingConnectionFactory implements BlockingConnectionFactory<TransferWrapBlockingConnection> {

        @Override
        public TransferWrapBlockingConnection makeConnection(SocketChannel channel, TypedFactory wrappedFactory) {
            return new TransferWrapBlockingConnection(channel, wrappedFactory);
        }
    }
    public static final Log LOG = LogFactory.getLog(TransferWrapBlockingConnection.class);
    
    private SocketChannel channel;
    private AtomicBoolean active;
    
    private String remote;
    private String host;
    private String ip;
    private int port;
    private boolean resolved;
  
    private TypedFactory wrappedFactory;

    public TransferWrapBlockingConnection(SocketChannel channel, TypedFactory wrappedFactory) {
        this.channel = channel;
        active = new AtomicBoolean(true);
        this.wrappedFactory = wrappedFactory;
        
        InetSocketAddress remoteAddr = Util.getRemoteAddr(channel.socket());
        if (!remoteAddr.isUnresolved()) {
            resolved = Util.isNameResolved(remoteAddr.getAddress());
            ip = remoteAddr.getAddress().getHostAddress();
        }
        host = remoteAddr.getHostName();
        port = remoteAddr.getPort();
        remote = host+ ":" + port;
    }
    
    @Override
    public int write(TransferWrap wrap) throws IOException {
        synchronized (channel) {
            return wrap.write(channel);
        }
    }

    @Override
    public boolean isActive() {
        return active.get();
    }

    @Override
    public boolean isResolved() {
        return resolved;
    }
    
    @Override
    public SocketChannel getChannel() {
        return channel;
    }

    @Override
    public TransferWrap read() throws IOException {
        TransferWrap readBuf = new TransferWrap(wrappedFactory);
        readBuf.read(channel);
        return readBuf;
    }

    @Override
    public void close() {
        active.getAndSet(false);
        synchronized (channel) {
            if (!channel.isOpen()) {
                return;
            }
            try {
                channel.socket().shutdownOutput();
            } catch (IOException e1) {}
            try {
                channel.close();
            } catch (IOException e) {}
            try {
                channel.socket().close();
            } catch (IOException e) {}
        }
    }
    
    public String getHost() {
        return host;
    }
    
    public String getIP() {
        return ip;
    }
    
    @Override
    public String toString() {
        return remote;
    }
}

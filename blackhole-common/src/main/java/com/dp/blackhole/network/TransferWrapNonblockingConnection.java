package com.dp.blackhole.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;

public class TransferWrapNonblockingConnection implements NonblockingConnection<TransferWrap> {

    public static class TransferWrapNonblockingConnectionFactory implements NonblockingConnectionFactory<TransferWrapNonblockingConnection> {

        @Override
        public TransferWrapNonblockingConnection makeConnection(SocketChannel channel, Selector selector, TypedFactory wrappedFactory) {
            return new TransferWrapNonblockingConnection(channel, selector, wrappedFactory);
        }
    }
    
    public static final Log LOG = LogFactory.getLog(TransferWrapNonblockingConnection.class);
    
    private Selector selector;
    private SocketChannel channel;
    private AtomicBoolean active;
    
    private ConcurrentLinkedQueue<TransferWrap> writeQueue;
    private TransferWrap readBuf;
    private boolean readComplete;
    private boolean writeComplete;
    
    private String remote;
    private String host;
    private String ip;
    private int port;
    private boolean resolved;
  
    private TypedFactory wrappedFactory;

    public TransferWrapNonblockingConnection(SocketChannel channel, Selector selector, TypedFactory wrappedFactory) {
        this.channel = channel;
        writeQueue = new ConcurrentLinkedQueue<TransferWrap>();
        active = new AtomicBoolean(true);
        this.selector = selector;
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
    public void send(TransferWrap entity) {
        if (!isActive()) {
            LOG.error("connection closed, message sending abort");
            return;
        }
        offer(entity);
        SelectionKey key = keyFor(selector);
        try {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        } catch (CancelledKeyException e) {
            LOG.warn("Exception while sending message." + e);
        }
        selector.wakeup();
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
    public SelectionKey keyFor(Selector selector) {
        return channel.keyFor(selector);
    }

    private void offer(TransferWrap buffer) {
        writeQueue.offer(buffer);
    }

    @Override
    public void readyforRead() {
        readComplete = false;
        readBuf = null;
    }    
    
    @Override
    public int read() throws IOException {
        if (readBuf == null) {
            readBuf = new TransferWrap(wrappedFactory);
        }
        int read = readBuf.read(channel);
        if (readBuf.complete()) {
            readComplete = true;
        }
        return read;
    }

    @Override
    public boolean readComplete() {
        return readComplete;
    }

    @Override
    public int write() throws IOException {
        int written = 0;
        writeComplete = false;
        while (true) {
            TransferWrap messageTosend = writeQueue.peek();
            // end of queue
            if (messageTosend == null) {
                writeComplete = true;
                break;
            }
            // finish one buffer
            if (messageTosend.complete()) {
                writeQueue.poll();
                continue;
            }
            // start to write one buffer until socket writebuffer full
            written += messageTosend.write(channel);
            if (!messageTosend.complete()) {
                break;
            }
        }
        return written;
    }

    @Override
    public boolean writeComplete() {
        return writeComplete;
    }

    @Override
    public TransferWrap getEntity() {
        return readBuf;
    }

    @Override
    public void close() {
        active.getAndSet(false);
        
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

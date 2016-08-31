package com.dp.blackhole.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;

public class ByteBufferNonblockingConnection implements NonblockingConnection<ByteBuffer> {

    public static class ByteBufferNonblockingConnectionFactory implements NonblockingConnectionFactory<ByteBufferNonblockingConnection> {

        @Override
        public ByteBufferNonblockingConnection makeConnection(SocketChannel channel,
                Selector selector, TypedFactory wrappedFactory) {
            return new ByteBufferNonblockingConnection(channel, selector);
        }
    }
    
    public static final Log LOG = LogFactory.getLog(ByteBufferNonblockingConnection.class);
    
    private SocketChannel channel;
    private ByteBuffer length;
    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;
    private ConcurrentLinkedQueue<ByteBuffer> writeQueue;
    private boolean writeComplete;
    private boolean readComplete;
    private AtomicBoolean active;
    private Selector selector;
    private String remote;
    private String host;
    private String ip;
    private int port;
    private boolean resolved;

    public ByteBufferNonblockingConnection(SocketChannel channel, Selector selector) {
        this.channel = channel;
        this.selector = selector;
        writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
        active = new AtomicBoolean(true);
        length = ByteBuffer.allocate(4);
        
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

    private void offer(ByteBuffer buffer) {
        writeQueue.offer(buffer);
    }

    @Override
    public void readyforRead() {
        length.clear();
        readBuffer.clear();
        readComplete = false;
    }    
    
    @Override
    public int read() throws IOException {
        int read = 0;
        if (length.hasRemaining()) {
           int num = channel.read(length);
           if (num < 0) {
               throw new IOException("end-of-stream reached");
           }
           read += num;
           if (length.hasRemaining()) {
               return read;
           } else {
               length.flip();
               int len = length.getInt();
               readBuffer = ByteBuffer.allocate(len);
           }
        }

        int num = channel.read(readBuffer);
        read += num;
        if (num < 0) {
            throw new IOException("end-of-stream reached");
        }
        if (!readBuffer.hasRemaining()) {
            readBuffer.flip();
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
            if (writeBuffer == null) {
                ByteBuffer buffer = writeQueue.peek();
                // end of queue
                if (buffer == null) {
                    writeComplete = true;
                    break;
                }
                writeBuffer = ByteBuffer.allocate(4 + buffer.capacity());
                writeBuffer.putInt(buffer.capacity());
                writeBuffer.put(buffer);
                writeBuffer.flip();
            }
            
            // finish one buffer
            if (writeBuffer.remaining() == 0) {
                writeQueue.poll();
                writeBuffer = null;
                continue;
            }
            // start to write one buffer until socket writebuffer full
            for (int i = 0; i < 16; i++) {
                int num = channel.write(writeBuffer);
                written += num;
                if (num != 0) {
                    break;
                }
            }
            if (writeBuffer.hasRemaining()) {
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
    public ByteBuffer getEntity() {
        return readBuffer.duplicate();
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

    @Override
    public void send(ByteBuffer entity) {
        if (!isActive()) {
            LOG.error("connection closed, message sending abort: " + this.toString());
            return;
        }
        offer(entity.duplicate());
        SelectionKey key = keyFor(selector);
        try {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        } catch (CancelledKeyException e) {
            LOG.warn("Exception while sending message." + e);
        }
        selector.wakeup();
    }

    public String getHost() {
        return host;
    }
    
    public String getIP() {
        return ip;
    }
    
    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return remote;
    }
}

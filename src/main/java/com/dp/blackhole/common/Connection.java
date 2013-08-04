package com.dp.blackhole.common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.dp.blackhole.common.gen.MessagePB.Message;

public class Connection {
    SocketChannel channel;
    
    private ByteBuffer length;
    private ByteBuffer data;
    private ByteBuffer writeBuffer;
    private ConcurrentLinkedQueue<Message> queue;
    private AtomicLong lastHeartBeat;
    private String host;
    
    public Connection (SocketChannel channel) throws IOException {
        this.host = ((InetSocketAddress)channel.socket().getRemoteSocketAddress()).getHostName();
        this.channel = channel;
        this.length = ByteBuffer.allocate(4);
        this.queue = new ConcurrentLinkedQueue<Message>();
        lastHeartBeat = new AtomicLong(Util.getTS());
    }
    
    public void offer (Message msg) {
        queue.offer(msg);
    }
    
    public Message poll() {
        return queue.poll();
    }
    
    public Message peek() {
        return queue.peek();
    }
    
    public void createDatabuffer(int size) {
        data = ByteBuffer.allocate(size);
    }

    public ByteBuffer createWritebuffer(int size) {
        writeBuffer = ByteBuffer.allocate(size);
        return writeBuffer;
    }
    
    public void writeMessage(Message reply) {
//        SelectionKey key = channel.keyFor(selector);
//        Connection connection = (Connection) key.attachment();
//        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
//        connection.offer(msg);
//        selector.wakeup();    
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public ByteBuffer getWritebuffer() {
        return writeBuffer;
    }

    public void resetWritebuffer() {
        writeBuffer = null;        
    }

    public void close() {
        if (!channel.isOpen()) {
            return;
        }
        try {
            channel.socket().shutdownOutput();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            channel.socket().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ByteBuffer getLengthBuffer() {
        return length;
    }

    public ByteBuffer getDataBuffer() {
        return data;
    }
    
    public void updateHeartBeat() {
        lastHeartBeat.getAndSet(Util.getTS());
    }

    public long getLastHeartBeat() {
        return lastHeartBeat.get();
    }
    
    public String getHost() {
       return host;
    }
}

package com.dp.blackhole.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.dp.blackhole.common.gen.MessagePB.Message;

public class Connection {
    public static final int APPNODE = 1;
    public static final int COLLECTORNODE = 2;
    
    SocketChannel channel;
    
    private ByteBuffer length;
    private ByteBuffer data;
    private ByteBuffer writeBuffer;
    private ConcurrentLinkedQueue<Message> queue;
    private AtomicLong lastHeartBeat;
    private AtomicBoolean active;
    private String host;
    private int NodeType;
    
    public Connection (SocketChannel channel) throws IOException {
        this.host = Util.getRemoteHost(channel.socket());
        this.channel = channel;
        this.length = ByteBuffer.allocate(4);
        this.queue = new ConcurrentLinkedQueue<Message>();
        lastHeartBeat = new AtomicLong(Util.getTS());
        active = new AtomicBoolean(true);
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
        active.getAndSet(false);
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
    
    public void setNodeType(int type) {
        NodeType = type;  
    }
    
    public int getNodeType( ) {
        return NodeType;  
    }
    
    public boolean isActive() {
        return active.get();
    }
    
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("host:" +host);
        if (NodeType == APPNODE) {
            sb.append(",type:APPNODE");
        } else if (NodeType == COLLECTORNODE) {
            sb.append(",type:COLLECTORNODE");
        } else {
            sb.append(",type:UNKNOWN");
        }
        return sb.toString();
    }
}

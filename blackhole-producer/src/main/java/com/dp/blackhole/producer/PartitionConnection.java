package com.dp.blackhole.producer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.TopicCommonMeta;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.protocol.data.HaltRequest;
import com.dp.blackhole.protocol.data.VersionRequest;
import com.dp.blackhole.protocol.data.ProduceRequest;
import com.dp.blackhole.protocol.data.RegisterRequest;
import com.dp.blackhole.protocol.data.RollRequest;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;

public class PartitionConnection {
    private static final Log LOG = LogFactory.getLog(PartitionConnection.class);
    private static final int DEFAULT_DELAY_SECONDS = 5;
    private String topic;
    private long rollPeriod;
    private String broker;
    private int brokerPort;
    public final String partitionId;
    private SocketChannel channel;
    private Socket socket;
    private ByteBuffer messageBuffer;
    private int messageNum;
    private long minMsgSent;
    private int reassignDelaySeconds = DEFAULT_DELAY_SECONDS;
    
    
    public PartitionConnection(TopicCommonMeta topicMeta, String topic, String broker, int port, String partitionId) {
        this.topic = topic;
        this.broker = broker;
        this.brokerPort = port;
        this.partitionId = partitionId;
        this.rollPeriod = topicMeta.getRollPeriod();
        this.minMsgSent = topicMeta.getMinMsgSent();
    }
    
    public String getPartitionId() {
        return partitionId;
    }

    public int getReassignDelaySeconds() {
        return reassignDelaySeconds;
    }

    public void setReassignDelaySeconds(int reassignDelaySeconds) {
        this.reassignDelaySeconds = reassignDelaySeconds;
    }

    public ByteBuffer getMessageBuffer() {
        return messageBuffer;
    }

    public void initializeRemoteConnection() throws IOException {
        messageBuffer = ByteBuffer.allocate(512 * 1024);
        channel = SocketChannel.open();
        channel.connect(new InetSocketAddress(broker, brokerPort));
        socket = channel.socket();
        LOG.info(topic + " with " + partitionId + " connected " + broker + ":" + brokerPort);
        doStreamReg();
    }
    
    private void doStreamReg() throws IOException {
        RegisterRequest request = new RegisterRequest(topic, partitionId, rollPeriod);
        TransferWrap wrap = new TransferWrap(request);
        wrap.write(channel);
    }
    
    public void sendRollRequest() throws IOException {
        RollRequest request = new RollRequest(topic, partitionId, rollPeriod);
        TransferWrap wrap = new TransferWrap(request);
        wrap.write(channel);
    }
    
    public void sendHaltRequest() throws IOException {
        HaltRequest request = new HaltRequest(topic, partitionId, rollPeriod);
        TransferWrap wrap = new TransferWrap(request);
        wrap.write(channel);
    }
    
    public void sendNullRequest() throws IOException {
        VersionRequest request = new VersionRequest(ParamsKey.VERSION);
        TransferWrap wrap = new TransferWrap(request);
        wrap.write(channel);
    }
    
    public synchronized void sendMessage() throws IOException {
        messageBuffer.flip();
        ByteBufferMessageSet messages = new ByteBufferMessageSet(messageBuffer.slice());
        ProduceRequest request = new ProduceRequest(topic, partitionId, messages);
        TransferWrap wrap = new TransferWrap(request);
        wrap.write(channel);
        messageBuffer.clear();
        messageNum = 0;
    }
    
    public void cacahAndSendLine(byte[] line) throws IOException {
        
        Message message = new Message(line); 
        
        if (messageNum >= minMsgSent || message.getSize() > messageBuffer.remaining()) {
            sendMessage();
        }
        
        message.write(messageBuffer);
        messageNum++;
    }
    
    public synchronized void close() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
                socket = null;
            }
        } catch (IOException e) {
            LOG.warn("Failed to close socket.", e);
        }
    }
}

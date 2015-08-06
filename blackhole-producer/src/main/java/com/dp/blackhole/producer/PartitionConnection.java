package com.dp.blackhole.producer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Sender;
import com.dp.blackhole.common.TopicCommonMeta;
import com.dp.blackhole.network.BlockingConnection;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.network.TypedFactory;
import com.dp.blackhole.network.TransferWrapBlockingConnection.TransferWrapBlockingConnectionFactory;
import com.dp.blackhole.protocol.data.DataMessageTypeFactory;
import com.dp.blackhole.protocol.data.HaltRequest;
import com.dp.blackhole.protocol.data.HeartBeatRequest;
import com.dp.blackhole.protocol.data.ProduceRequest;
import com.dp.blackhole.protocol.data.RegisterRequest;
import com.dp.blackhole.protocol.data.RollRequest;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;

public class PartitionConnection implements Sender {
    private static final Log LOG = LogFactory.getLog(PartitionConnection.class);
    private static final int DEFAULT_DELAY_SECONDS = 5;
    private String topic;
    private long rollPeriod;
    private String broker;
    private int brokerPort;
    public final String partitionId;
    private BlockingConnection<TransferWrap> connection;
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

    public void initializeRemoteConnection() throws IOException {
        messageBuffer = ByteBuffer.allocate(512 * 1024);
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(true);
        SocketAddress server = new InetSocketAddress(broker, brokerPort);
        socketChannel.connect(server);
        LOG.info(topic + " with " + partitionId + " connected " + broker + ":" + brokerPort);
        TransferWrapBlockingConnectionFactory factory = new TransferWrapBlockingConnectionFactory();
        TypedFactory wrappedFactory = new DataMessageTypeFactory();
        connection = factory.makeConnection(socketChannel, wrappedFactory);
        doStreamReg();
    }
    
    private void doStreamReg() throws IOException {
        RegisterRequest request = new RegisterRequest(topic, partitionId, rollPeriod, broker);
        TransferWrap wrap = new TransferWrap(request);
        connection.write(wrap);
    }
    
    public void sendRollRequest() throws IOException {
        RollRequest request = new RollRequest(topic, partitionId, rollPeriod);
        TransferWrap wrap = new TransferWrap(request);
        connection.write(wrap);
    }
    
    public void sendHaltRequest() throws IOException {
        HaltRequest request = new HaltRequest(topic, partitionId, rollPeriod);
        TransferWrap wrap = new TransferWrap(request);
        connection.write(wrap);
    }
    
    @Override
    public void heartbeat() throws IOException {
        HeartBeatRequest request = new HeartBeatRequest(ParamsKey.SOCKET_HEARTBEAT);
        TransferWrap wrap = new TransferWrap(request);
        connection.write(wrap);
    }
    

    @Override
    public String getSource() {
        return partitionId;
    }

    @Override
    public String getTarget() {
        return this.broker + ":" + this.brokerPort;
    }

    @Override
    public boolean canSend() {
        return messageBuffer.position() != 0;
    }
    
    @Override
    public synchronized void sendMessage() throws IOException {
        synchronized (messageBuffer) {
            if (canSend()) {
                messageBuffer.flip();
                ByteBufferMessageSet messages = new ByteBufferMessageSet(messageBuffer.slice());
                ProduceRequest request = new ProduceRequest(topic, partitionId, messages);
                TransferWrap wrap = new TransferWrap(request);
                connection.write(wrap);
                messageBuffer.clear();
                messageNum = 0;
            }
        }
    }
    
    public void cacahAndSendLine(byte[] line) throws IOException {
        
        Message message = new Message(line); 
        
        if (messageNum >= minMsgSent || message.getSize() > messageBuffer.remaining()) {
            sendMessage();
        }
        
        message.write(messageBuffer);
        messageNum++;
    }
    
    @Override
    public void close() {
        connection.close();
    }

    @Override
    public boolean isActive() {
        return connection.isActive();
    }
}

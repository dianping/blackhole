package com.dp.blackhole.agent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.agent.AgentMeta.TopicId;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Sender;
import com.dp.blackhole.network.BlockingConnection;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.network.TypedFactory;
import com.dp.blackhole.network.TransferWrapBlockingConnection.TransferWrapBlockingConnectionFactory;
import com.dp.blackhole.protocol.data.DataMessageTypeFactory;
import com.dp.blackhole.protocol.data.HaltRequest;
import com.dp.blackhole.protocol.data.HeartBeatRequest;
import com.dp.blackhole.protocol.data.ProduceRequest;
import com.dp.blackhole.protocol.data.ProducerRegReply;
import com.dp.blackhole.protocol.data.RegisterRequest;
import com.dp.blackhole.protocol.data.RollRequest;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;

public class RemoteSender implements Sender{
    private static final Log LOG = LogFactory.getLog(RemoteSender.class);
    private TopicId topicId;
    private long rollPeriod;
    private String source;
    private String broker;
    private int brokerPort;
    private BlockingConnection<TransferWrap> connection;
    private ByteBuffer messageBuffer;
    private int messageNum;
    private int minMsgSent;
    private int msgBufSize;
    private int reassignDelaySeconds = Agent.DEFAULT_DELAY_SECONDS;
    
    public RemoteSender(AgentMeta topicMeta, String broker, int port) {
        this.topicId = topicMeta.getTopicId();
        this.source = topicMeta.getSource();
        this.broker = broker;
        this.brokerPort = port;

        this.rollPeriod = topicMeta.getRollPeriod();
        this.minMsgSent = topicMeta.getMinMsgSent();
        this.msgBufSize = topicMeta.getMsgBufSize();
    }
    
    public TopicId getTopicId() {
        return topicId;
    }
    
    public String getBroker() {
        return broker;
    }
    
    public int getReassignDelaySeconds() {
        return reassignDelaySeconds;
    }

    public void setReassignDelaySeconds(int reassignDelaySeconds) {
        this.reassignDelaySeconds = reassignDelaySeconds;
    }

    public boolean initializeRemoteConnection() throws IOException {
        messageBuffer = ByteBuffer.allocate(msgBufSize);
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(true);
        SocketAddress server = new InetSocketAddress(broker, brokerPort);
        socketChannel.connect(server);
        LOG.info(topicId + " connected broker: " + broker + ":" + brokerPort);
        TransferWrapBlockingConnectionFactory factory = new TransferWrapBlockingConnectionFactory();
        TypedFactory wrappedFactory = new DataMessageTypeFactory();
        connection = factory.makeConnection(socketChannel, wrappedFactory);
        doStreamReg();
        TransferWrap response = connection.read();//TODO good design is set read timeout
        ProducerRegReply reply = (ProducerRegReply) response.unwrap();
        return reply.getResult();
    }
    
    private void doStreamReg() throws IOException {
        RegisterRequest request = new RegisterRequest(
                topicId.getTopic(),
                source,
                rollPeriod,
                broker);
        TransferWrap wrap = new TransferWrap(request);
        connection.write(wrap);
    }
    
    public void sendRollRequest() throws IOException {
        RollRequest request = new RollRequest(
                topicId.getTopic(),
                source,
                rollPeriod);
        TransferWrap wrap = new TransferWrap(request);
        connection.write(wrap);
    }
    
    public void sendHaltRequest() throws IOException {
        HaltRequest request = new HaltRequest(
                topicId.getTopic(),
                source,
                rollPeriod);
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
        return this.source;
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
    public void sendMessage() throws IOException {
        synchronized (messageBuffer) {
            if (canSend()) {
                messageBuffer.flip();
                ByteBufferMessageSet messages = new ByteBufferMessageSet(messageBuffer.slice());
                ProduceRequest request = new ProduceRequest(topicId.getTopic(), source, messages);
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
        
        synchronized (messageBuffer) {
            message.write(messageBuffer);
        }
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

package com.dp.blackhole.agent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.agent.AgentMeta.TopicId;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.StreamConnection;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.protocol.data.HaltRequest;
import com.dp.blackhole.protocol.data.VersionRequest;
import com.dp.blackhole.protocol.data.ProduceRequest;
import com.dp.blackhole.protocol.data.RegisterRequest;
import com.dp.blackhole.protocol.data.RollRequest;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;

public class RemoteSender implements StreamConnection {
    private static final Log LOG = LogFactory.getLog(RemoteSender.class);
    private TopicId topicId;
    private long rollPeriod;
    private String source;
    private String broker;
    private int brokerPort;
    private SocketChannel channel;
    private ByteBuffer messageBuffer;
    private int messageNum;
    private long minMsgSent;
    private int reassignDelaySeconds = Agent.DEFAULT_DELAY_SECONDS;
    
    public RemoteSender(AgentMeta topicMeta, String broker, int port) {
        this.topicId = topicMeta.getTopicId();
        this.source = topicMeta.getSource();
        this.broker = broker;
        this.brokerPort = port;
        this.rollPeriod = topicMeta.getRollPeriod();
        this.minMsgSent = topicMeta.getMinMsgSent();
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

    public void initializeRemoteConnection() throws IOException {
        messageBuffer = ByteBuffer.allocate(512 * 1024);
        channel = SocketChannel.open();
        channel.connect(new InetSocketAddress(broker, brokerPort));
        channel.socket().setKeepAlive(true);
        LOG.info(topicId + " connected broker: " + broker + ":" + brokerPort);
        doStreamReg();
    }
    
    private void doStreamReg() throws IOException {
        RegisterRequest request = new RegisterRequest(
                topicId.getTopic(),
                source,
                rollPeriod,
                broker);
        TransferWrap wrap = new TransferWrap(request);
        syncWrite(wrap);
    }
    
    public void sendRollRequest() throws IOException {
        RollRequest request = new RollRequest(
                topicId.getTopic(),
                source,
                rollPeriod);
        TransferWrap wrap = new TransferWrap(request);
        syncWrite(wrap);
    }
    
    public void sendHaltRequest() throws IOException {
        HaltRequest request = new HaltRequest(
                topicId.getTopic(),
                source,
                rollPeriod);
        TransferWrap wrap = new TransferWrap(request);
        syncWrite(wrap);
    }
    
    public void sendNullRequest() throws IOException {
        VersionRequest request = new VersionRequest(ParamsKey.VERSION);
        TransferWrap wrap = new TransferWrap(request);
        syncWrite(wrap);
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
    public void sendSignal() throws IOException {
        sendNullRequest();
    }
    
    @Override
    public void sendMessage() throws IOException {
        synchronized (messageBuffer) {
            if (canSend()) {
                messageBuffer.flip();
                ByteBufferMessageSet messages = new ByteBufferMessageSet(messageBuffer.slice());
                ProduceRequest request = new ProduceRequest(topicId.getTopic(), source, messages);
                TransferWrap wrap = new TransferWrap(request);
                syncWrite(wrap);
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
    
    private void syncWrite(TransferWrap wrap) throws IOException {
        synchronized (channel) {
            wrap.write(channel);
        }
    }
    
    @Override
    public void close() {
        synchronized (channel) {
            try {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                    channel.socket().close();
                    channel = null;
                }
            } catch (IOException e) {
                LOG.warn("Failed to close socket.", e);
            }
        }
    }
}

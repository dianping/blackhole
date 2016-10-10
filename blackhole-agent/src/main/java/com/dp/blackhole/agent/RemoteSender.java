package com.dp.blackhole.agent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.agent.AgentMeta.TopicId;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Sender;
import com.dp.blackhole.network.BlockingConnection;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.network.TransferWrapBlockingConnection.TransferWrapBlockingConnectionFactory;
import com.dp.blackhole.network.TypedFactory;
import com.dp.blackhole.protocol.data.DataMessageTypeFactory;
import com.dp.blackhole.protocol.data.HaltRequest;
import com.dp.blackhole.protocol.data.HeartBeatRequest;
import com.dp.blackhole.protocol.data.MessageAck;
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
    private OffsetInfo offsetInfo;
    private long initOffset;
    private Date lastSend;
    
    public RemoteSender(AgentMeta topicMeta, String broker, int port) {
        this.topicId = topicMeta.getTopicId();
        this.source = topicMeta.getSource();
        this.broker = broker;
        this.brokerPort = port;

        this.rollPeriod = topicMeta.getRollPeriod();
        this.minMsgSent = topicMeta.getMinMsgSent();
        this.msgBufSize = topicMeta.getMsgBufSize();
        this.lastSend = new Date();
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

    public void setOffsetInfo(OffsetInfo offsetInfo) throws IOException {
        this.offsetInfo = offsetInfo;
        TreeMap<Long, ByteBuffer> reSend = this.offsetInfo.initOffset(this.initOffset);
        for(Entry<Long, ByteBuffer> entry : reSend.entrySet()) {
            ByteBufferMessageSet messages = new ByteBufferMessageSet(entry.getValue());
            LOG.debug("re-send messages offset: " +  entry.getKey() + ", message set size: " + messages.getValidSize());
            ProduceRequest request = new ProduceRequest(topicId.getTopic(), source, messages,
                    entry.getKey());
            TransferWrap wrap = new TransferWrap(request);
            connection.write(wrap);
        }
        while (offsetInfo.getMsgQueueSize() >= ParamsKey.TopicConf.DEFAULT_MESSAGE_QUEUE_MIN_ACK) {
            TransferWrap response = connection.read();
            MessageAck ackReply = (MessageAck) response.unwrap();
            LOG.debug("Received message ack of Topic: " + topicId + ", Partition: " + source + ", Offset: "
                    + ackReply.getOffset());
            this.offsetInfo.msgAck(ackReply.getOffset());
        }
    }

    public boolean initializeRemoteConnection() {
        boolean res = false;
        SocketChannel socketChannel = null;
        try {
            messageBuffer = ByteBuffer.allocate(msgBufSize);
            socketChannel = SocketChannel.open();
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
            LOG.info("Topic: " + topicId.getTopic() + " Partition: " + source + " InitOffest: " + reply.getOffset()
                    + " reg with " + broker + " was done.");
            res = reply.getResult();
            if (res) {
                this.initOffset = reply.getOffset();
            }
        } catch (Exception e) {
            LOG.error("initializeRemoteConnection exception", e);
        }
        if (!res) {
            if (socketChannel != null) {
                if (socketChannel.isOpen()) {
                    try {
                        socketChannel.close();
                    } catch (IOException e) {
                        LOG.error("socket channel close exception", e);
                    }
                    try {
                        socketChannel.socket().close();
                    } catch (IOException e) {
                        LOG.error("socket close exception", e);
                    }
                }
            }
        }
        return res;
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
        return messageBuffer.position() != 0
                && offsetInfo.getMsgQueueSize() < ParamsKey.TopicConf.DEFAULT_MESSAGE_QUEUE_MIN_ACK;
    }
    
    @Override
    public void sendMessage() throws IOException {
        synchronized (messageBuffer) {
            if (messageBuffer.position() == 0) {
                this.lastSend = new Date();
                this.lastSend.setTime(this.lastSend.getTime() + 1000L);
                return;
            }
            messageBuffer.flip();
            ByteBuffer bb = messageBuffer.slice();
            ByteBufferMessageSet messages = new ByteBufferMessageSet(bb);
            long currentOffset = this.offsetInfo.getCurrentOffset();
            this.offsetInfo.newMsg(bb, messages.getValidSize());
            ProduceRequest request = new ProduceRequest(topicId.getTopic(), source, messages, currentOffset);
            TransferWrap wrap = new TransferWrap(request);
            while (messageNum != 0) {
                connection.write(wrap);
                LOG.debug("send message to leader: " + broker + " for topic: " + topicId + ", Partition: " + source
                        + ", offset: " + currentOffset + ", message set size: " + messages.getValidSize());
                messageBuffer.clear();
                messageNum = 0;
                this.lastSend = new Date();
                this.lastSend.setTime(this.lastSend.getTime() + 1000L);
            }
            while (offsetInfo.getMsgQueueSize() >= ParamsKey.TopicConf.DEFAULT_MESSAGE_QUEUE_MIN_ACK) {
                TransferWrap response = connection.read();
                MessageAck ackReply = (MessageAck) response.unwrap();
                LOG.debug("Received message ack of Topic: " + topicId + ", Partition: " + source + ", Offset: "
                        + ackReply.getOffset());
                this.offsetInfo.msgAck(ackReply.getOffset());
            }
        }
    }
    
    public void cacahAndSendLine(byte[] line) throws IOException {
        Message message = new Message(line); 
        Date date = new Date();
        
        if (date.compareTo(this.lastSend) >= 0 || message.getSize() > messageBuffer.remaining()) {
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

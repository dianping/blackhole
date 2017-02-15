package com.dp.blackhole.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.RollPartition;
import com.dp.blackhole.broker.storage.StorageManager;
import com.dp.blackhole.broker.storage.StorageManager.Reporter.ReportEntry;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.TopicPartitionKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenServer;
import com.dp.blackhole.network.NioService;
import com.dp.blackhole.network.NonblockingConnectionFactory;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.network.TransferWrapNonblockingConnection;
import com.dp.blackhole.network.TypedFactory;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.data.DataMessageTypeFactory;
import com.dp.blackhole.protocol.data.FetchReply;
import com.dp.blackhole.protocol.data.FetchRequest;
import com.dp.blackhole.protocol.data.HaltRequest;
import com.dp.blackhole.protocol.data.MessageAck;
import com.dp.blackhole.protocol.data.MultiFetchReply;
import com.dp.blackhole.protocol.data.MultiFetchRequest;
import com.dp.blackhole.protocol.data.OffsetReply;
import com.dp.blackhole.protocol.data.OffsetRequest;
import com.dp.blackhole.protocol.data.ProduceRequest;
import com.dp.blackhole.protocol.data.ProducerRegReply;
import com.dp.blackhole.protocol.data.RegisterRequest;
import com.dp.blackhole.protocol.data.ReplicaFetchRep;
import com.dp.blackhole.protocol.data.ReplicaFetchReq;
import com.dp.blackhole.protocol.data.RollRequest;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.FileMessageSet;
import com.dp.blackhole.storage.MessageAndOffset;
import com.dp.blackhole.storage.MessageSet;

public class BrokerService extends Thread {
    private final Log LOG = LogFactory.getLog(BrokerService.class);
    
    GenServer<TransferWrap, TransferWrapNonblockingConnection, EntityProcessor<TransferWrap, TransferWrapNonblockingConnection>> server;
    StorageManager manager;
    PublisherExecutor executor;
    private Map<TransferWrapNonblockingConnection, ClientDesc> clients;
    int servicePort;
    int numHandler;
    String localhost;
    ConcurrentHashMap<TopicPartitionKey, TransferWrapNonblockingConnection> tpKeyConn;
    ConcurrentHashMap<TopicPartitionKey, OriginMeta> tpKeyOriginMeta;
    double INSYNC_THRESHOLD_DEFAULT;
    double INSYNC_BUFFER_DEFAULT;
    int MAX_TOLERANCE_DEFAULT;
    
    public static void reportPartitionInfo(List<ReportEntry> entrylist) {
        Broker.getSupervisor().reportPartitionInfo(entrylist);
    }
    
    @Override
    public void run() {
        try {
            server.init("Publisher", servicePort, numHandler);
        } catch (IOException e) {
            LOG.error("Failed to init GenServer", e);
        }
    }
    
    public BrokerService(Properties prop) throws IOException {
        this.setName("BrokerService");
        localhost = Util.getLocalHost();
        String storagedir = prop.getProperty("broker.storage.dir");
        int splitThreshold = Integer.parseInt(prop.getProperty("broker.storage.splitThreshold", "536870912"));
        int flushThreshold = Integer.parseInt(prop.getProperty("broker.storage.flushThreshold", "4194304"));
        numHandler = Integer.parseInt(prop.getProperty("GenServer.handler.count", "3"));
        servicePort = Integer.parseInt(prop.getProperty("broker.service.port"));
        INSYNC_THRESHOLD_DEFAULT = Double.parseDouble(prop.getProperty("broker.follower.insyncThreshold", "0.5"));
        INSYNC_BUFFER_DEFAULT = Double.parseDouble(prop.getProperty("broker.follower.insyncBuffer", "0.3"));
        MAX_TOLERANCE_DEFAULT = Integer.parseInt(prop.getProperty("broker.follower.insyncMaxTolerance", "10"));
        clients = new ConcurrentHashMap<TransferWrapNonblockingConnection, BrokerService.ClientDesc>();
        tpKeyConn = new ConcurrentHashMap<TopicPartitionKey, TransferWrapNonblockingConnection>();
        tpKeyOriginMeta = new ConcurrentHashMap<TopicPartitionKey, OriginMeta>();
        manager = new StorageManager(storagedir, splitThreshold, flushThreshold);
        executor = new PublisherExecutor();
        NonblockingConnectionFactory<TransferWrapNonblockingConnection> factory = new TransferWrapNonblockingConnection.TransferWrapNonblockingConnectionFactory();
        TypedFactory wrappedFactory = new DataMessageTypeFactory();
        server = new GenServer<TransferWrap, TransferWrapNonblockingConnection, EntityProcessor<TransferWrap, TransferWrapNonblockingConnection>>
            (executor, factory, wrappedFactory);
    }
    
    public PublisherExecutor getExecutor() {
        return executor;
    }
    
    public StorageManager getPersistentManager() {
        return manager;
    }
    
    public void disconnectClients() {
        for (TransferWrapNonblockingConnection client : clients.keySet()) {
            server.closeConnection(client);
        }
    }
    
    public void stopLeaderIfExists(TopicPartitionKey tpKey) throws IOException {
        server.closeConnection(tpKeyConn.get(tpKey));
        OriginMeta om = this.tpKeyOriginMeta.get(tpKey);
        if (om != null) {
            synchronized (om) {
                om.deactive();
                om.setLeaderOffline();
                this.tpKeyOriginMeta.remove(tpKey);
                om.flush();
            }
        }
    }

    public void addLeader(String topic, String partition, HashMap<String, Boolean> followerStatus) {
        TopicPartitionKey tpKey = new TopicPartitionKey(topic, partition);
        OriginMeta om = tpKeyOriginMeta.get(tpKey);
        if (om == null) {
            Partition p = manager.getPartition(topic, partition);
            if (p == null) {
                LOG.fatal("couldn't find partition topic: " + topic + ", partition: " + partition);
                return;
            }
            long initOffset = p.getEndOffset();
            om = new OriginMeta(topic, partition, localhost, followerStatus, initOffset, p, this.INSYNC_BUFFER_DEFAULT,
                    this.INSYNC_BUFFER_DEFAULT, this.MAX_TOLERANCE_DEFAULT);
            tpKeyOriginMeta.put(tpKey, om);
        }
    }

    private boolean isEqualEntropy(int newEntropy, TopicPartitionKey tpKey) {
        int oldEntropy = this.manager.getEntropy(tpKey.getTopic(), tpKey.getPartition());
        LOG.debug("checEqualEntropy: compare new entropy: " + newEntropy + " and old entropy: "
                + oldEntropy + " for topic: " + tpKey.getTopic() + ", partition: " + tpKey.getPartition());
        return newEntropy == oldEntropy ? true : false;
    }

    protected void handleFollowerSyncStatusAck(TopicPartitionKey tpKey, String follower, boolean syncStatus,
            boolean ack) {
        OriginMeta originMeta = tpKeyOriginMeta.get(tpKey);
        if (originMeta == null) {
            LOG.error("OriginMeta doesn't exist when handling followerSyncStatusAck Topic: " + tpKey.getTopic()
                    + ", Partition: " + tpKey.getPartition());
            return;
        }
        synchronized (originMeta) {
            if (syncStatus) {
                if (originMeta.isFollowerReadyInSync(follower)) {
                    originMeta.setFollowerSyncStatus(follower, syncStatus);
                } else {
                    LOG.error("follower's status changed before syncstatus: " + syncStatus + " ack, skip re-send.");
                }
            } else {
                if (originMeta.isFollowerReadyOutOfSync(follower)) {
                    originMeta.setFollowerSyncStatus(follower, syncStatus);
                } else {
                    LOG.error("follower's status changed before syncstatus: " + syncStatus + " ack, skip re-send.");
                }
            }
        }
    }

    protected void handleFollowerSyncStatusTimeout(int entropy, TopicPartitionKey tpKey, String follower,
            boolean syncStatus) {
        OriginMeta originMeta = tpKeyOriginMeta.get(tpKey);
        if (originMeta == null) {
            LOG.error("OriginMeta doesn't exist when handling followerSyncStatusTimeout Topic: " + tpKey.getTopic()
                    + ", Partition: " + tpKey.getPartition());
            return;
        }
        synchronized (originMeta) {
            if (syncStatus) {
                if (originMeta.isFollowerReadyInSync(follower)) {
                    if (originMeta.checkFollowerStillInSync(follower)) {
                        sendFollowerSyncStatus(entropy, tpKey.getTopic(), tpKey.getPartition(), follower, syncStatus);
                    } else {
                        LOG.error("follower: " + follower + " fall behind with Topic: " + tpKey.getTopic()
                                + ", Partition: " + tpKey.getPartition() + ", skip re-send");
                        originMeta.rollbackFollowerStatus(follower);
                    }
                } else {
                    LOG.error("follower's status changed before syncstatus: " + syncStatus + " timeout, skip re-send.");
                }
            } else {
                if (originMeta.isFollowerReadyOutOfSync(follower)) {
                    if (originMeta.checkFollowerStillOutOfSync(follower)) {
                        sendFollowerSyncStatus(entropy, tpKey.getTopic(), tpKey.getPartition(), follower, syncStatus);
                    } else {
                        LOG.error("follower: " + follower + " catch up with Topic: " + tpKey.getTopic()
                                + ", Partition: " + tpKey.getPartition() + ", skip re-send");
                        originMeta.rollbackFollowerStatus(follower);
                    }
                } else {
                    LOG.error("follower's status changed before syncstatus: " + syncStatus + " timeout, skip re-send.");
                }
            }
        }
    }

    private void sendFollowerSyncStatus(int entropy, String topic, String partition, String follower,
            boolean status) {
        Message msg = PBwrap.wrapFollowerSyncStatus(entropy, topic, partition, follower, status);
        Broker.getSupervisor().sendWithExpect(Message.MessageType.FOLLOWER_SYNC_STATUS_ACK_VALUE, msg, msg.getCallId());
    }

    public class PublisherExecutor implements
            EntityProcessor<TransferWrap, TransferWrapNonblockingConnection> {
        
        private void closeClientOfErrorRequest(TransferWrapNonblockingConnection from, Object request) {
            LOG.info("can't find topic/partition with " + request + " from " + from +" ,close connection");
            server.closeConnection(from);
        }

        public void handleRegisterRequest(RegisterRequest request, TransferWrapNonblockingConnection from) {
            TopicPartitionKey tpKey = new TopicPartitionKey(request.topic, request.partitionId);
            OriginMeta om = tpKeyOriginMeta.get(tpKey);
            if (om == null || !om.isActive()) {
                ProducerRegReply reply = new ProducerRegReply(false, -1L);
                from.send(new TransferWrap(reply));
                return;
            }
            clients.put(from, new ClientDesc(request.topic, ClientDesc.AGENT, request.partitionId));
            tpKeyConn.put(tpKey, from);
            Partition p = manager.getPartition(request.topic, request.partitionId);
            ProducerRegReply reply = null;
            if (p != null) {
                Message msg = PBwrap.wrapReadyStream(request.topic, request.partitionId, request.period, localhost,
                        Util.getTS());
                Broker.getSupervisor().send(msg);
                reply = new ProducerRegReply(true, om.getEndOffset());
            } else {
                LOG.fatal("couldn't find partition topic: " + tpKey.getTopic() + ", partition: " + tpKey.getPartition()
                        + " when handling resgiter reqeust");
                reply = new ProducerRegReply(false, -1L);
            }
            om.setLeaderOnline();
            from.send(new TransferWrap(reply));
        }

        public void handleProduceRequest(ProduceRequest request,
                TransferWrapNonblockingConnection from) {
            LOG.debug("Received produce request Topic:" + request.topic + ", Partition: " + request.partitionId
                    + ", Offset: " + request.offset + ", MessageSetSize: " + request.getMessageSize());
            String topic = request.topic;
            String partition = request.partitionId;
            TopicPartitionKey tpKey = new TopicPartitionKey(topic, partition);
            Partition p = manager.getPartition(request.topic, request.partitionId);
            OriginMeta originMeta = tpKeyOriginMeta.get(tpKey);
            if (p != null && originMeta != null) {
                synchronized (originMeta) {
                    try {
                        boolean adjustOffset = false;
                        if (request.offset > originMeta.getEndOffset()) {
                            LOG.fatal("Message may missing.\n" + "RequestOffset: " + request.offset + ".\n"
                                    + originMeta.toString());
                            adjustOffset = false;
                        }
                        if (request.offset < originMeta.getEndOffset()) {
                            LOG.fatal("Redundant messages.\n" + "RequestOffset: " + request.offset + ".\n"
                                    + originMeta.toString());
                            adjustOffset = false;
                        }
                        if (adjustOffset) {
                            long offset = originMeta.getEndOffset();
                            LOG.debug("Need to adjust offset for Topic: " + topic + ", Partition: " + partition
                                    + ", Offset: " + offset);
                            MessageAck repAck = new MessageAck(false, offset);
                            TransferWrapNonblockingConnection conn = tpKeyConn.get(tpKey);
                            if (conn != null) {
                                conn.send(new TransferWrap(repAck));
                            }
                            return;
                        }
                        originMeta.append((ByteBufferMessageSet) request.getMesssageSet());
                        originMeta.newMessage();
                    } catch (IOException e) {
                        LOG.error("IOE catched", e);
                    }
                }
            } else {
                Util.logError(LOG, null, "can not get partition", request.topic, request.partitionId);
            }
        }

        public void handleFetchRequest(FetchRequest request,
                TransferWrapNonblockingConnection from) {
            Partition p = manager.getPartition(request.topic, request.partitionId);
            if (p == null) {
                closeClientOfErrorRequest(from, request);
                return;
            }
            FileMessageSet messages = p.read(request.offset, request.limit);
            
            TransferWrap reply = null;
            if (messages == null) {
                reply = new TransferWrap(new FetchReply(p.getId(), messages, MessageAndOffset.OFFSET_OUT_OF_RANGE));
                LOG.warn("Found offset out of range for " + request);
            } else {
                reply = new TransferWrap(new FetchReply(p.getId(), messages, request.offset));
            }
            from.send(reply);
        }

        public void handleMultiFetchRequest(MultiFetchRequest request,
                TransferWrapNonblockingConnection from) {
            ArrayList<String> partitionList = new ArrayList<String>();
            ArrayList<MessageSet> messagesList = new ArrayList<MessageSet>();
            ArrayList<Long> offsetList = new ArrayList<Long>();
            for (FetchRequest f : request.fetches) {
                Partition p = manager.getPartition(f.topic, f.partitionId);
                if (p == null) {
                    closeClientOfErrorRequest(from, request);
                    return;
                }
                FileMessageSet messages = p.read(f.offset, f.limit);
                partitionList.add(p.getId());
                messagesList.add(messages);
                offsetList.add(messages.getOffset());
            }

            from.send(new TransferWrap(new MultiFetchReply(partitionList,
                    messagesList, offsetList)));
        }

        public void handleOffsetRequest(OffsetRequest request,
                TransferWrapNonblockingConnection from) {
            Partition p = manager.getPartition(request.topic, request.partition);
            if (p == null) {
                closeClientOfErrorRequest(from, request);
                return;
            }
            if (request.autoOffset == OffsetRequest.EARLIES_OFFSET) {
                from.send(new TransferWrap(new OffsetReply(request.topic, request.partition, p.getStartOffset())));
            } else {
                from.send(new TransferWrap(new OffsetReply(request.topic, request.partition, p.getEndOffset())));
            }
        }
        
        public void handleRollRequest(RollRequest request, TransferWrapNonblockingConnection from) {
            Partition p = manager.getPartition(request.topic, request.partitionId);
            if (p == null) {
                closeClientOfErrorRequest(from, request);
                return;
            }
            try {
                RollPartition roll = p.markRollPartition();
                Broker.getRollMgr().perpareUpload(request.topic, request.partitionId, request.rollPeriod, roll);
            } catch (IOException e) {
                LOG.error("Got an IOE", e);
            }
        }
        
        public void handleLastRotateRequest(HaltRequest request, TransferWrapNonblockingConnection from) {
            Partition p = manager.getPartition(request.topic, request.partitionId);
            if (p == null) {
                closeClientOfErrorRequest(from, request);
                return;
            }
            try {
                RollPartition roll = p.markRollPartition();
                Broker.getRollMgr().doClean(request.topic, request.partitionId, request.rollPeriod, roll);
            } catch (IOException e) {
                LOG.error("Got an IOE", e);
            }
        }

        public void handleReplicaFetchReq(ReplicaFetchReq request, TransferWrapNonblockingConnection from) {
            int entropy = request.getEntropy();
            String leader = request.getBrokerLeader();
            String follower = request.getBrokerReplica();
            String topic = request.getTopic();
            String partition = request.getPartition();
            long offset = request.getOffset();
            long id = request.getId();
            LOG.debug("Received replica fetch request Leader: " + leader + ", Replica: " + follower + ", Topic: "
                    + topic + ", Partition: " + partition + ", Offset: " + offset + ", FetchId: " + id);
            TopicPartitionKey tpKey = new TopicPartitionKey(topic, partition);
            OriginMeta originMeta = tpKeyOriginMeta.get(tpKey);
            if (originMeta == null) {
                LOG.info("Leader not ready. Send empty replicaFetchReply to follower: " + follower + ", Topic: "
                        + topic + ", Partition: " + partition + ", Offset: " + offset);
                sendReplicaFetchRep(from, entropy, topic, partition, leader, follower, offset, null, id);
                return;
            }
            if (!isEqualEntropy(entropy, tpKey)) {
                LOG.info("Not a valid entropy. Send empty replicaFetchReply to follower: " + follower + ", Topic: "
                        + topic + ", Partition: " + partition + ", Offset: " + offset);
                sendReplicaFetchRep(from, entropy, topic, partition, leader, follower, offset, null, id);
                return;
            }
            synchronized (originMeta) {
                if (!tpKeyOriginMeta.containsKey(tpKey) || !originMeta.isLeaderOnline()
                        || !originMeta.containsFolower(follower) || !originMeta.isActive()) {
                    sendReplicaFetchRep(from, entropy, topic, partition, leader, follower, offset, null, id);
                    return;
                }
                Partition p = manager.getPartition(topic, partition);
                FileMessageSet messages = null;
                HashMap<String, Boolean> syncStatusChanges = null;
                // TODO these rules may not cover all the situations when follower has errors about offset
                if (offset < originMeta.getFollowerOffset(follower) || offset < p.getStartOffset() || offset > p.getEndOffset()) {
                    LOG.fatal("Follower offset error.\n" + "Follower: " + follower + ", Offset: " + offset + ".\n"
                            + originMeta.toString());
                    originMeta.initFollowerOffset(follower);
                    messages = p.read(p.getStartOffset(), ParamsKey.TopicConf.DEFAULT_REPLICA_FETCHSIZE);
                    offset = p.getStartOffset();
                } else {
                    messages = p.read(offset, ParamsKey.TopicConf.DEFAULT_REPLICA_FETCHSIZE);
                    syncStatusChanges = originMeta.getStatusChange(follower, offset);
                }
                long newLeo = originMeta.getLeo();
                if (syncStatusChanges != null) {
                    for (Entry<String, Boolean> change : syncStatusChanges.entrySet()) {
                        sendFollowerSyncStatus(p.getEntropy(), topic, partition, change.getKey(), change.getValue());
                    }
                }
                LOG.debug("Send Message ACK, " + "Topic: " + topic + ", Partition: " + partition + ", Offset: " + newLeo
                        + "\n" + originMeta.toString());
                MessageAck repAck = new MessageAck(true, newLeo);
                TransferWrapNonblockingConnection conn = tpKeyConn.get(tpKey);
                if (conn != null) {
                    conn.send(new TransferWrap(repAck));
                }
                sendReplicaFetchRep(from, entropy, topic, partition, leader, follower, offset, messages, id);
            }
        }

        private void sendReplicaFetchRep(TransferWrapNonblockingConnection conn, int entropy, String topic,
                String partition, String leader, String follower, long offset, FileMessageSet messages, long id) {
            if (conn != null) {
                LOG.debug("Send ReplicaFetchReply to follower " + follower + ", Topic: " + topic + ", Partition: "
                        + partition + ", offset: " + offset + ", size: " + (messages == null ? 0 : messages.getSize())
                        + ", FetchId: " + id);
                TransferWrap reply = new TransferWrap(
                        new ReplicaFetchRep(entropy, topic, partition, leader, follower, offset, messages, id));
                conn.send(reply);
            }
        }

        @Override
        public void process(TransferWrap request, TransferWrapNonblockingConnection from) {
            switch (request.getType()) {
            case DataMessageTypeFactory.RegisterRequest:
                handleRegisterRequest((RegisterRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.ProduceRequest:
                handleProduceRequest((ProduceRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.RotateOrRollRequest:
                handleRollRequest((RollRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.LastRotateRequest:
                handleLastRotateRequest((HaltRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.MultiFetchRequest:
                handleMultiFetchRequest((MultiFetchRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.FetchRequest:
                handleFetchRequest((FetchRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.OffsetRequest:
                handleOffsetRequest((OffsetRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.Heartbeat:
                break;
            case DataMessageTypeFactory.ReplicaFetchReq:
                handleReplicaFetchReq((ReplicaFetchReq) request.unwrap(), from);
                break;
            default:
                LOG.error("unknown message type: " + request.getType());
            }
        }

        @Override
        public void OnConnected(TransferWrapNonblockingConnection connection) {
            LOG.info(connection + " connected");
            clients.put(connection, new ClientDesc(ClientDesc.CONSUMER));
        }

        @Override
        public void OnDisconnected(TransferWrapNonblockingConnection connection) {
            LOG.info(connection + " disconnected");
            ClientDesc desc = clients.get(connection);
            if (desc.type == ClientDesc.AGENT) {
                tpKeyConn.remove(new TopicPartitionKey(desc.topic, desc.source));
                Broker.getRollMgr().reportFailure(desc.topic, desc.source, Util.getTS());
            }
            clients.remove(connection);
        }

        @Override
        public void receiveTimout(TransferWrap msg, TransferWrapNonblockingConnection conn) {

        }

        @Override
        public void sendFailure(TransferWrap msg, TransferWrapNonblockingConnection conn) {

        }

        @Override
        public void setNioService(NioService<TransferWrap, TransferWrapNonblockingConnection> service) {

        }

    }

    class ClientDesc {
        public static final int AGENT = 0;
        public static final int CONSUMER = 1;
        
        public String topic;
        public int type;
        public String source;
        
        public ClientDesc (String topic, int type, String source) {
            this.topic = topic;
            this.type = type;
            this.source = source;
        }
        
        public ClientDesc(int type) {
            this.type = type;
        }
    }
    
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("GenServer.handlercount", "1");
        properties.setProperty("GenServer.port", "2222");
        properties.setProperty("broker.storage.dir", "/tmp/base");
        BrokerService pubservice = new BrokerService(properties);
        pubservice.run();
    }
}

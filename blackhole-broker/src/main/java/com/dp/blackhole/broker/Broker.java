package com.dp.blackhole.broker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;

import com.dianping.cat.Cat;
import com.dp.blackhole.broker.follower.FollowerConsumer;
import com.dp.blackhole.broker.ftp.FTPConfigrationLoader;
import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.StorageManager.Reporter.ReportEntry;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.TopicPartitionKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.ByteBufferNonblockingConnection;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.HeartBeat;
import com.dp.blackhole.network.NioService;
import com.dp.blackhole.protocol.control.FollowerSyncStatusAckPB.FollowerSyncStatusAck;
import com.dp.blackhole.protocol.control.FollowerSyncStatusPB.FollowerSyncStatus;
import com.dp.blackhole.protocol.control.LeoReportPB.LeoReport;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.MessagePB.Message.MessageType;
import com.dp.blackhole.protocol.control.ReplicaExceptionPB.ReplicaException;
import com.dp.blackhole.protocol.control.RollIDPB.RollID;
import com.dp.blackhole.protocol.control.TopicReportPB.TopicReport;
import com.dp.blackhole.protocol.control.TransitionToFollowerPB.TransitionToFollower;
import com.dp.blackhole.protocol.control.TransitionToLeaderPB.TransitionToLeader;
import com.dp.blackhole.protocol.control.TransitionToLeaderPB.TransitionToLeader.Follower;
import com.google.protobuf.InvalidProtocolBufferException;

public class Broker {
    final static Log LOG = LogFactory.getLog(Broker.class);
    
    private static Broker broker;
    private static BrokerService brokerService;
    private static RollManager rollMgr;
    private static String version = Util.getVersion();
    private BrokerProcessor processor;
    private ByteBufferNonblockingConnection supervisor;
    private GenClient<ByteBuffer, ByteBufferNonblockingConnection, BrokerProcessor> client;
    private int servicePort;
    private int recoveryPort;
    private FollowerConsumer followerConsumer;
    private int supervisorSendTimeout;
    
    public Broker() throws IOException {
        rollMgr = new RollManager();
        broker = this;
    }

    private void start() throws FileNotFoundException, IOException {
        Properties prop = new Properties();
        prop.load(getClass().getClassLoader().getResourceAsStream("config.properties"));
        String supervisorHost = prop.getProperty("supervisor.host");
        int supervisorPort = Integer.parseInt(prop.getProperty("supervisor.port"));
        servicePort =  Integer.parseInt(prop.getProperty("broker.service.port"));
        recoveryPort = Integer.parseInt(prop.getProperty("broker.recovery.port"));
        supervisorSendTimeout = Integer.parseInt(prop.getProperty("broker.supervisor.sendTimeout", "10"));
        String hdfsbasedir = prop.getProperty("broker.hdfs.basedir");
        String copmressionAlgoName = prop.getProperty("broker.hdfs.compression.default");
        long clockSyncBufMillis = Long.parseLong(prop.getProperty("broker.rollmanager.clockSyncBufMillis", String.valueOf(ParamsKey.DEFAULT_CLOCK_SYNC_BUF_MILLIS)));
        int maxUploadThreads = Integer.parseInt(prop.getProperty("broker.rollmanager.maxUploadThreads", "20"));
        int maxRecoveryThreads = Integer.parseInt(prop.getProperty("broker.rollmanager.maxRecoveryThreads", "10"));
        int recoverySocketTimeout = Integer.parseInt(prop.getProperty("broker.rollmanager.recoverySocketTimeout", "600000"));

        boolean enableSecurity = Boolean.parseBoolean(prop.getProperty("broker.hdfs.security.enable", "true"));
        if (enableSecurity) {
            String keytab = prop.getProperty("broker.blackhole.keytab");
            String principal = prop.getProperty("broker.blackhole.principal");
            Configuration conf = new Configuration();
            conf.set("broker.blackhole.keytab", keytab);
            conf.set("broker.blackhole.principal", principal);
            HDFSLogin(conf, "broker.blackhole.keytab", "broker.blackhole.principal");
        }
        
        boolean enableFTP = Boolean.parseBoolean(prop.getProperty("broker.storage.ftp.enable", "false"));
        if (enableFTP) {
            long ftpConfigrationCheckInterval = Long.parseLong(prop.getProperty("broker.storage.ftp.configCheckIntervalMilli", "600000"));
            FTPConfigrationLoader ftpConfigrationLoader =  new FTPConfigrationLoader(ftpConfigrationCheckInterval);
            Thread thread = new Thread(ftpConfigrationLoader);
            thread.start();
        }
        
        Cat.logEvent("startup", version);
        LOG.info("Broker startup, version " + version);
        rollMgr.init(hdfsbasedir, copmressionAlgoName, recoveryPort, clockSyncBufMillis, maxUploadThreads, maxRecoveryThreads, recoverySocketTimeout);
        
        brokerService = new BrokerService(prop);
        brokerService.setDaemon(true);
        brokerService.start();
        followerConsumer = new FollowerConsumer(brokerService.manager, prop);
        
        // start GenClient
        processor = new BrokerProcessor();
        client = new GenClient(
                processor,
                new ByteBufferNonblockingConnection.ByteBufferNonblockingConnectionFactory(),
                null);
        client.init("broker", supervisorHost, supervisorPort);
        
        rollMgr.close();
    }
    
    private void registerNode() {
        send(PBwrap.wrapBrokerReg(servicePort, recoveryPort));
        LOG.info("register broker node with supervisor");
    }

    private void HDFSLogin(Configuration conf, String keytab, String principle) throws IOException {        
        SecurityUtil.login(conf, keytab, principle);
    }
    
    public void reportPartitionInfo(List<ReportEntry> entrylist) {
        List<TopicReport.TopicEntry> topicEntryList = new ArrayList<TopicReport.TopicEntry>(entrylist.size());
        for (ReportEntry entry : entrylist) {
            TopicReport.TopicEntry topicEntry = PBwrap.getTopicEntry(entry.topic, entry.partition, entry.offset);
            topicEntryList.add(topicEntry);
        }
        send(PBwrap.wrapTopicReport(topicEntryList));
    }
    
    public void sendLeoReply(String callId, int entropy, String topic, String partition, long offset) {
        Message msgLeoReply = PBwrap.wrapLeoReply(callId, entropy, topic, partition, offset);
        LOG.info("sending LeoReply to supervisor for entropy: " + entropy + ", topic: " + topic + ", partition: "
                + partition + ", leo: " + offset);
        send(msgLeoReply);
    }

    public void send(Message msg) {
        if (msg.getType() != MessageType.TOPIC_REPORT) {
            LOG.debug("send: " + msg);
        }
        Util.send(supervisor, msg);
    }

    public void sendWithExpect(int expect, Message msg, String callId) {
        LOG.debug("send: " + msg);
        if (supervisor == null) {
            LOG.error("peer is not connected, message sending abort " + msg);
            return;
        }
        processor.service.sendWithExpect(callId, supervisor, PBwrap.PB2Buf(msg), expect, supervisorSendTimeout,
                TimeUnit.SECONDS);
    }

    public static Broker getSupervisor() {
        return broker;
    }
    
    public static RollManager getRollMgr() {
        return rollMgr;
    }
    
    public static BrokerService getBrokerService() {
        return brokerService;
    }
    
    class BrokerProcessor implements EntityProcessor<ByteBuffer, ByteBufferNonblockingConnection> {
        private HeartBeat heartbeat = null;
        private NioService<ByteBuffer, ByteBufferNonblockingConnection> service = null;
        
        @Override
        public void OnConnected(ByteBufferNonblockingConnection connection) {
            supervisor = connection;          
            registerNode();
            heartbeat = new HeartBeat(supervisor, version);
            heartbeat.start();
        }

        @Override
        public void OnDisconnected(ByteBufferNonblockingConnection connection) {
            supervisor = null;
            brokerService.disconnectClients();
            LOG.debug("Last HeartBeat ts is " + heartbeat.getLastHeartBeat());
            heartbeat.shutdown();
            heartbeat = null;
        }

        @Override
        public void receiveTimout(ByteBuffer msg, ByteBufferNonblockingConnection conn) {
            Message msgPB = null;
            try {
                msgPB = PBwrap.Buf2PB(msg);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("InvalidProtocolBufferException catched ", e);
                return;
            }

            switch (msgPB.getType()) {
            case FOLLOWER_SYNC_STATUS:
                handleFollowerSyncStatusTimeout(msgPB.getFollowerSyncStatus());
                break;
            default:
                LOG.warn("unknown message: " + msg.toString());
            }
        }

        @Override
        public void sendFailure(ByteBuffer msg, ByteBufferNonblockingConnection conn) {

        }

        @Override
        public void setNioService(NioService<ByteBuffer, ByteBufferNonblockingConnection> service) {
            this.service = service;
        }

        @Override
        public void process(ByteBuffer buf, ByteBufferNonblockingConnection from) {
            Message message = null;;
            try {
                message = PBwrap.Buf2PB(buf);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("InvalidProtocolBufferException catched: ", e);
                return;
            }
            
            if (message.getType() != Message.MessageType.FOLLOWER_SYNC_STATUS_ACK) {
                LOG.debug("received: " + message);
            }
            RollID rollID = null;
            switch (message.getType()) {
            case UPLOAD_ROLL:
                rollID = message.getRollID();
                rollMgr.doUpload(rollID);
                break;
            case MAKR_UNRECOVERABLE:
                rollID = message.getRollID();
                rollMgr.markUnrecoverable(rollID);
                break;
            case TRANSITION_TO_FOLLOWER:
                handleTransitionToFollower(message.getTransitionToFollower());
                break;
            case TRANSITION_TO_LEADER:
                handleTransitionToLeader(message.getTransitionToLeader(), from);
                break;
            case REPLICA_EXCEPTION:
                handleReplicaException(message.getReplicaException(), from);
                break;
            case LEO_REPORT:
                handleLeoReport(message.getCallId(), message.getLeoReport(), from);
                break;
            case FOLLOWER_SYNC_STATUS_ACK:
                boolean accept = (this.service.unwatch(message.getCallId(), from,
                        Message.MessageType.FOLLOWER_SYNC_STATUS_ACK_VALUE) != null) ? true : false;
                LOG.debug("received(" + (accept ? "accept" : "ignore") + "): " + message);
                if (accept) {
                    handleFollowerSyncStatusAck(message.getFollowerSyncStatusAck(), from);
                }
                break;
            default:
                LOG.error("response type is undefined");
                break;
            }
        }

        private void handleReplicaException(ReplicaException re, ByteBufferNonblockingConnection from) {
            TopicPartitionKey tpKey = new TopicPartitionKey(re.getTopic(), re.getPartition());
            if (!isValidEntropy(re.getEntropy(), tpKey.getTopic(), tpKey.getPartition())) {
                return;
            }
            try {
                brokerService.stopLeaderIfExists(tpKey);
            } catch (IOException e) {
                LOG.error("error happened when stopping leader topic: " + tpKey.getTopic() + ", partition: "
                        + tpKey.getPartition());
                e.printStackTrace();
                return;
            }
        }

        private void handleTransitionToLeader(TransitionToLeader transitionToLeader, ByteBufferNonblockingConnection from) {
            TopicPartitionKey tpKey = new TopicPartitionKey(transitionToLeader.getTopic(), transitionToLeader.getPartition());
            createPartitionIfExists(transitionToLeader.getTopic(), transitionToLeader.getPartition(), transitionToLeader.getLeaderOffset());
            if (!isValidEntropy(transitionToLeader.getEntropy(), tpKey.getTopic(), tpKey.getPartition())) {
                return;
            }
            brokerService.manager.updateEntropy(tpKey.getTopic(), tpKey.getPartition(), transitionToLeader.getEntropy());
            try {
                followerConsumer.stopFollowerIfExists(tpKey);
            } catch (IOException e) {
                LOG.error("error happened when stopping follower topic: " + tpKey.getTopic() + ", partition: "
                        + tpKey.getPartition());
                e.printStackTrace();
                return;
            }
            HashMap<String, Boolean> followerStatus = new HashMap<String, Boolean>();
            List<Follower> followers = transitionToLeader.getFollowersList();
            for (Follower f : followers) {
                followerStatus.put(f.getFollower(), f.getStatus());
            }
            brokerService.addLeader(transitionToLeader.getTopic(), transitionToLeader.getPartition(), followerStatus);
        }

        private void handleTransitionToFollower(TransitionToFollower transitionToFollower) {
            TopicPartitionKey tpKey = new TopicPartitionKey(transitionToFollower.getTopic(),
                    transitionToFollower.getPartition());
            createPartitionIfExists(tpKey.getTopic(), tpKey.getPartition(), transitionToFollower.getOffset());
            if (!isValidEntropy(transitionToFollower.getEntropy(), tpKey.getTopic(), tpKey.getPartition())) {
                return;
            }
            boolean truncate = needTruncate(transitionToFollower.getEntropy(), tpKey.getTopic(), tpKey.getPartition());
            brokerService.manager.updateEntropy(tpKey.getTopic(), tpKey.getPartition(), transitionToFollower.getEntropy());
            String brokerLeader = transitionToFollower.getBrokerLeader();
            int brokerLeaderPort = transitionToFollower.getBrokerPort();
            try {
                brokerService.stopLeaderIfExists(tpKey);
                // TODO add this line will cause race condition when a broker received transition_to_follower twice, need a better solution
//                followerConsumer.stopFollowerIfExists(tpKey);
            } catch (IOException e) {
                LOG.error("error happened when stopping leader topic: " + tpKey.getTopic() + ", partition: "
                        + tpKey.getPartition());
                e.printStackTrace();
            }
            Partition p = brokerService.manager.getPartition(tpKey.getTopic(), tpKey.getPartition());
            if (truncate) {
                try {
                    p.truncate(transitionToFollower.getOffset());
                } catch (IOException e) {
                    LOG.error("fail to re-init segement topic: " + tpKey.getTopic() + ", partition: "
                            + tpKey.getPartition());
                    e.printStackTrace();
                }
            }
            followerConsumer.addReplica(brokerLeader, brokerLeaderPort, tpKey, p);
        }

        private void handleLeoReport(String callId, LeoReport leoReport, ByteBufferNonblockingConnection from) {
            TopicPartitionKey tpKey = new TopicPartitionKey(leoReport.getTopic(), leoReport.getPartition());
            if(!isValidEntropy(leoReport.getEntropy(), tpKey.getTopic(), tpKey.getPartition())) {
                return;
            }
            try {
                brokerService.stopLeaderIfExists(tpKey);
                followerConsumer.stopFollowerIfExists(tpKey);
            } catch (IOException e) {
                LOG.error("error happened when stopping leader/follower topic: " + tpKey.getTopic() + ", partition: "
                        + tpKey.getPartition());
                e.printStackTrace();
                return;
            }
            long leo = brokerService.manager.getLogEndOffset(tpKey.getTopic(), tpKey.getPartition());
            if (leo != -1) {
                sendLeoReply(callId, leoReport.getEntropy(), tpKey.getTopic(), tpKey.getPartition(), leo);
            }
        }

        private void handleFollowerSyncStatusAck(FollowerSyncStatusAck followerSyncStatusAck,
                ByteBufferNonblockingConnection from) {
            TopicPartitionKey tpKey = new TopicPartitionKey(followerSyncStatusAck.getTopic(),
                    followerSyncStatusAck.getPartitionId());
            int entropy = followerSyncStatusAck.getEntropy();
            if (!isEqualEntropy(entropy, tpKey.getTopic(), tpKey.getPartition())) {
                return;
            }
            brokerService.handleFollowerSyncStatusAck(tpKey, followerSyncStatusAck.getFollower(),
                    followerSyncStatusAck.getSyncStatus(), followerSyncStatusAck.getAck());
        }

        private void handleFollowerSyncStatusTimeout(FollowerSyncStatus followerSyncStatus) {
            TopicPartitionKey tpKey = new TopicPartitionKey(followerSyncStatus.getTopic(),
                    followerSyncStatus.getPartitionId());
            int entropy = followerSyncStatus.getEntropy();
            if (!isEqualEntropy(entropy, tpKey.getTopic(), tpKey.getPartition())) {
                return;
            }
            brokerService.handleFollowerSyncStatusTimeout(entropy, tpKey, followerSyncStatus.getFollower(),
                    followerSyncStatus.getSyncStatus());
        }

        private void createPartitionIfExists(String topic, String partition, long offset) {
            Partition p = brokerService.manager.getPartition(topic, partition);
            if (p == null) {
                try {
                    // TODO add a message to support one topic leader failure
                    if (!brokerService.manager.createPartition(topic, partition)) {
                        return;
                    }
                    p = brokerService.manager.getPartition(topic, partition);
                    p.truncate(offset);
                } catch (IOException e) {
                    LOG.error("error when creating partition Topic: " + topic + ", Partition: " + partition
                            + ", offset: " + offset);
                    e.printStackTrace();
                    return;
                }
            }
        }

        private boolean isValidEntropy(int newEntropy, String topic, String partition) {
            int oldEntropy = brokerService.manager.getEntropy(topic, partition);
            LOG.debug("ValidEntropy: compare new entropy: " + newEntropy + " and old entropy: " + oldEntropy
                    + " for topic: " + topic + ", partition: " + partition);
            return newEntropy >= oldEntropy ? true : false;
        }

        private boolean isEqualEntropy(int newEntropy, String topic, String partition) {
            int oldEntropy = brokerService.manager.getEntropy(topic, partition);
            LOG.debug("EqualEntropy: compare new entropy: " + newEntropy + " and old entropy: " + oldEntropy
                    + " for topic: " + topic + ", partition: " + partition);
            return newEntropy == oldEntropy ? true : false;
        }

        private boolean needTruncate(int newEntropy, String topic, String partition) {
            int oldEntropy = brokerService.manager.getEntropy(topic, partition);
            LOG.debug("NeedTruncate: compare new entropy: " + newEntropy + " and old entropy: " + oldEntropy
                    + " for topic: " + topic + ", partition: " + partition);
            return newEntropy > oldEntropy ? true : false;
        }
    }

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
    try {
        Broker broker = new Broker();
        broker.start();
    } catch (Exception e) {
            LOG.error("fatal error ", e);
            System.exit(-1);
        }
    }
}
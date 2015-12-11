package com.dp.blackhole.producer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dp.blackhole.common.LingeringSender;
import com.dp.blackhole.common.DaemonThreadFactory;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.TopicCommonMeta;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.HeartBeat;
import com.dp.blackhole.network.ByteBufferNonblockingConnection;
import com.dp.blackhole.protocol.control.AssignBrokerPB.AssignBroker;
import com.dp.blackhole.protocol.control.AssignPartitionPB.AssignPartition;
import com.dp.blackhole.protocol.control.CommonConfResPB.CommonConfRes;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.MessagePB.Message.MessageType;
import com.dp.blackhole.protocol.control.NoAvailableNodePB.NoAvailableNode;
import com.dp.blackhole.protocol.control.NoavailableConfPB.NoavailableConf;
import com.dp.blackhole.protocol.control.ProducerIdAssignPB.ProducerIdAssign;
import com.dp.blackhole.protocol.control.RecoveryRollPB.RecoveryRoll;
import com.google.protobuf.InvalidProtocolBufferException;

public class ProducerConnector implements Runnable {
    private final Log LOG = LogFactory.getLog(ProducerConnector.class);
    private static final int FIVE_MINUTES = 5 * 60;
    private static final long DEFAULT_PRODUCER_ROLL_PERIOD = 3600;
    //this is a static instance, concurrency of all actions should be take to account
    private static ProducerConnector instance = new ProducerConnector();
    private LingeringSender streamHealthChecker;
    
    public static ProducerConnector getInstance() {
        return instance;
    }
    
    private AtomicBoolean initialized = new AtomicBoolean(false);
    ByteBufferNonblockingConnection supervisor;
    private volatile ScheduledThreadPoolExecutor scheduler;
    
    
    private ConcurrentHashMap<String, LinkedBlockingQueue<Producer>> unAssignIdProducers;
    // registered producers
    private ConcurrentHashMap<String, Map<String, Producer>> workingProducers;
    
    
    private GenClient<ByteBuffer, ByteBufferNonblockingConnection, ProducerProcessor> client;
    private ProducerProcessor processor;
    private String supervisorHost;
    private int supervisorPort;
    
    public ProducerConnector() {
        unAssignIdProducers = new ConcurrentHashMap<String, LinkedBlockingQueue<Producer>>();
        workingProducers = new ConcurrentHashMap<String, Map<String,Producer>>();
    }
    
    public LingeringSender getStreamHealthChecker() {
        return streamHealthChecker;
    }

    public boolean isInitialized() {
        return initialized.get();
    }
    
    private void launchScheduler() {
        scheduler = new ScheduledThreadPoolExecutor(1, new DaemonThreadFactory("Scheduler"));
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }
    
    public void init(Properties prop) {
        synchronized (instance) {
            if (isInitialized()) {
                return;
            }
            String lingerMs = null;
            String configSource = null;
            String supervisorHost = null;
            String supervisorPort = null;
            if (prop != null) {
                configSource = "CODE";
                supervisorHost = prop.getProperty("supervisor.host");
                supervisorPort = prop.getProperty("supervisor.port");
                lingerMs       = prop.getProperty("producer.linger.ms");
            }
            if (supervisorHost == null || supervisorPort == null) {
                configSource = "LION";
                ConfigCache configCache = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
                supervisorHost = configCache.getProperty("blackhole.supervisor.host");
                supervisorPort = configCache.getProperty("blackhole.supervisor.port");
            }
            if (supervisorHost == null || supervisorPort == null) {
                throw new RuntimeException("Can not get supervisorHost or supervisorPort");
            }
            LOG.info("get connection " + supervisorHost + ":" + supervisorPort + " from " + configSource);
            this.supervisorHost = supervisorHost;
            this.supervisorPort = Integer.parseInt(supervisorPort);
            processor = new ProducerProcessor();
            launchScheduler();
            launchHealthStreamChecker(lingerMs);
            launch();
            initialized.getAndSet(true);
        }
    }

    private void launch() {
        Thread thread = new Thread(instance);
        thread.setDaemon(true);
        thread.start();
    }

    private void launchHealthStreamChecker(String lingerMs) {
        streamHealthChecker = new LingeringSender(Util.parseInt(lingerMs, LingeringSender.DEFAULT_LINGER_MS));
        streamHealthChecker.start();
    }

    public void prepare(Producer producer) {
        unAssignIdProducers.putIfAbsent(producer.getTopic(), new LinkedBlockingQueue<Producer>());
        LinkedBlockingQueue<Producer> producers = unAssignIdProducers.get(producer.getTopic());
        producers.offer(producer);
        
        workingProducers.putIfAbsent(producer.getTopic(), new ConcurrentHashMap<String, Producer>());
        processor.requireConfigFromSupersivor(producer.getTopic(), 0);
    }
    
    public void producerReg(String topic, String producerId, int delaySecond) {
        Message msg = PBwrap.wrapProducerReg(topic, producerId);
        LOG.info("register producer " + topic);
        send(msg, delaySecond);
    }
    
    private void requireBrokerForPartition(String topic, String produerId,
            String partitionId, int reassignDelay) {
        Message msg = PBwrap.wrapPartitionBrokerRequire(topic, produerId, partitionId);
        LOG.info("partition " + partitionId + " require a broker");
        send(msg, reassignDelay);
    }

    public void reportPartitionConnectionFailure(String topic, String produerId,
            String partitionId, long ts, int reassignDelay) {
        Message message = PBwrap.wrapProducerFailure(topic, partitionId, ts);
        send(message);
        requireBrokerForPartition(topic, produerId, partitionId, reassignDelay);
    }

    private void send(Message message, int delaySecond) {
        scheduler.schedule(new SendTask(message), delaySecond, TimeUnit.SECONDS);
    }
    
    public void send(Message message) {
        LOG.debug("send: " + message);
        Util.send(supervisor, message);
    }

    class SendTask implements Runnable {
        private Message msg;

        public SendTask(Message msg) {
            this.msg = msg;
        }

        @Override
        public void run() {
            send(msg);
        }
    }
    
    @Override
    public void run() {
        client = new GenClient(
                processor,
                new ByteBufferNonblockingConnection.ByteBufferNonblockingConnectionFactory(),
                null);

        try {
            client.init("producer", supervisorHost, supervisorPort);
        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
        }
    }
    
    public class ProducerProcessor implements EntityProcessor<ByteBuffer, ByteBufferNonblockingConnection> {

        private HeartBeat heartbeat;

        
        public void ConfReq(String topic, int delaySecond) {
            Message msg = PBwrap.wrapConfReq(topic);
            LOG.info("Request a config for " + topic + " after " + delaySecond + " seconds");
            send(msg, delaySecond);
        }
        
        @Override
        public void OnConnected(ByteBufferNonblockingConnection connection) {
            LOG.info("ProducerConnector connected");
            supervisor = connection;
            heartbeat = new HeartBeat(supervisor);
            heartbeat.start();
        }

        @Override
        public void OnDisconnected(ByteBufferNonblockingConnection connection) {
            LOG.info("ProducerConnector disconnected");
            supervisor = null;
            heartbeat.shutdown();
            heartbeat = null;
        }

        @Override
        public void process(ByteBuffer buffer, ByteBufferNonblockingConnection connection) {
            Message msg = null;
            try {
                msg = PBwrap.Buf2PB(buffer);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("received a message can not be deserialized", e);
            }

            LOG.debug("producer processor received: " + msg);         
            processInternal(msg);
        }

        private void processInternal(Message msg) {
            String topic;
            Map<String, Producer> producerMap;
            String producerId;
            MessageType type = msg.getType();
            switch (type) {
            case NOAVAILABLECONF:
                NoavailableConf noavailableConf = msg.getNoavailableConf();
                topic = noavailableConf.getTopic();
                requireConfigFromSupersivor(topic, FIVE_MINUTES);
                break;
            case PRODUCER_ID_ASSIGN:
                ProducerIdAssign producerIdAssign = msg.getProducerIdAssign();
                topic = producerIdAssign.getTopic();
                producerId = producerIdAssign.getProducerId();
                LinkedBlockingQueue<Producer> unassign_producers = unAssignIdProducers.get(topic);
                Producer producer = unassign_producers.poll();
                if (producer == null) {
                    LOG.warn("There is no unassign id producer to handle for topic " + topic);
                    break;
                }
                CommonConfRes commonConfRes = producerIdAssign.getConfRes();
                //set topic common meta
                TopicCommonMeta topicMeta = new TopicCommonMeta(
                        commonConfRes.getRollPeriod(),
                        commonConfRes.getMaxLineSize(),
                        commonConfRes.getMinMsgSent(),
                        commonConfRes.getMsgBufSize(),
                        commonConfRes.getPartitionFactor());
                producer.setTopicMeta(topicMeta);
                producer.setProducerId(producerId);
                producerMap = workingProducers.get(topic);
                producerMap.put(producerId, producer);
                
                producerReg(topic, producerId, 0);
                break;
            case NOAVAILABLENODE:
                NoAvailableNode noAvailableNode = msg.getNoAvailableNode();
                topic = noAvailableNode.getTopic();
                producerId = noAvailableNode.getSource();
                producerReg(topic, producerId, 0);
                break;
            case ASSIGN_PARTITION:
                AssignPartition assignPartition = msg.getAssignPartition();
                List<AssignBroker> assigns = assignPartition.getAssignsList();
                for (AssignBroker assignBroker : assigns) {
                    topic = assignBroker.getTopic();
                    producerMap = workingProducers.get(topic);
                    if (producerMap == null) {
                        LOG.error("There is no working producers register for topic " + topic);
                        continue;
                    }
                    String broker = assignBroker.getBrokerServer();
                    int brokerPort = assignBroker.getBrokerPort();
                    String partitionId = assignBroker.getPartitionId();
                    producerId = Util.getProducerIdFromPartitionId(partitionId);
                    Producer p = producerMap.get(producerId);
                    PartitionConnection partitionConnection = new PartitionConnection(p.geTopicMeta(), topic, broker, brokerPort, partitionId);
                    try {
                        partitionConnection.initializeRemoteConnection();
                    } catch (IOException e) {
                        LOG.error("init remote connection fail, register again.", e);
                        producerReg(topic, producerId, 0);
                        continue;
                    }
                    p.assignPartitionConnection(partitionConnection);
                    streamHealthChecker.register(topic, partitionId, partitionConnection);
                }
                break;
            case RECOVERY_ROLL:
                //TODO just return unrecoverable until broker HA completed.
                RecoveryRoll recoveryRoll = msg.getRecoveryRoll();
                topic = recoveryRoll.getTopic();
                producerId = recoveryRoll.getSource();
                long rollTs = recoveryRoll.getRollTs();
                boolean isFinal = recoveryRoll.getIsFinal();
                boolean persistent = recoveryRoll.getPersistent();
                long period = DEFAULT_PRODUCER_ROLL_PERIOD;
                producerMap = workingProducers.get(topic);
                if (producerMap != null) {
                    producer = producerMap.get(producerId);
                    if (producer != null) {
                        period = producer.geTopicMeta().getRollPeriod();
                    }
                }
                send(PBwrap.wrapUnrecoverable(topic, producerId, period, rollTs, isFinal, persistent));
                break;
            default:
                break;
            }
        }
        
        public void requireConfigFromSupersivor(String topic, int delaySecond) {
            Message msg = PBwrap.wrapConfReq(topic);
            LOG.info("Require a configuration for " + topic + " after " + delaySecond + " seconds.");
            send(msg, delaySecond);
        }
    }
}

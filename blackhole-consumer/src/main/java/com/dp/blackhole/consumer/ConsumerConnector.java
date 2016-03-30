package com.dp.blackhole.consumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dianping.lion.client.LionException;
import com.dp.blackhole.common.DaemonThreadFactory;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.consumer.api.Consumer;
import com.dp.blackhole.consumer.api.ConsumerConfig;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.HeartBeat;
import com.dp.blackhole.network.ByteBufferNonblockingConnection;
import com.dp.blackhole.protocol.control.AssignConsumerPB.AssignConsumer;
import com.dp.blackhole.protocol.control.AssignConsumerPB.AssignConsumer.PartitionOffset;
import com.dp.blackhole.protocol.control.ConsumerRegPB.ConsumerReg;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.MessagePB.Message.MessageType;
import com.dp.blackhole.storage.MessageAndOffset;
import com.google.protobuf.InvalidProtocolBufferException;

public class ConsumerConnector implements Runnable {
    private final Log LOG = LogFactory.getLog(ConsumerConnector.class);
    
    private static ConsumerConnector instance = new ConsumerConnector();
    private static String version = Util.getVersion();
    
    public static ConsumerConnector getInstance() {
        return instance;
    }
    
    public volatile boolean initialized = false;
    
    // consumerIdString->[fetcherrunable1, fetcherrunable2]
    private Map<String, List<Fetcher>> consumerThreadsMap;
    // registered consumers
    private ConcurrentHashMap<String, Consumer> consumers;   
    private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new DaemonThreadFactory("scheduler"));

    private GenClient<ByteBuffer, ByteBufferNonblockingConnection, ConsumerProcessor> client;
    private ConsumerProcessor processor;
    private ByteBufferNonblockingConnection supervisor;
    private String supervisorHost;
    private int supervisorPort;
    private boolean autoCommit;
    private int autoCommitIntervalMs;
    private boolean sentReg;
    
    private Map<String, ConsumerConfig> configMap;
    
    private ConsumerConnector() {
        configMap = Collections.synchronizedMap(new HashMap<String, ConsumerConfig>());
        consumerThreadsMap = new ConcurrentHashMap<String, List<Fetcher>>();
        consumers = new ConcurrentHashMap<String, Consumer>();
    }
    
    public synchronized void init(ConsumerConfig config) throws LionException {
        String configSource = null;
        String host = config.getSupervisorHost();
        int port = config.getSupervisorPort();
        if (host != null && port != 0) {
            configSource = "CODE";
        } else {
            try {
                Properties prop = new Properties();
                prop.load(getClass().getClassLoader().getResourceAsStream("consumer.properties"));
                host = prop.getProperty("supervisor.host");
                port = Integer.parseInt(prop.getProperty("supervisor.port"));
                configSource = "RESOURCE";
            } catch (Exception e) {
                try {
                    ConfigCache configCache = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
                    host = configCache.getProperty("blackhole.supervisor.host");
                    port = configCache.getIntProperty("blackhole.supervisor.port");
                    configSource = "LION";
                } catch (Exception le) {
                    LOG.warn("Finally, getting config from lion faild too", le);
                }
            }
        }
        
        if (host == null || port == 0 || configSource == null) {
            throw new RuntimeException("Can not get supervisorHost or supervisorPort");
        }
        LOG.info("get connection " + host + ":" + port + " from " + configSource);
        
        init(host, port, true, 6000);
    }

    public synchronized void init(String supervisorHost, int supervisorPort, boolean autoCommit, int autoCommitIntervalMs) {
        if (!instance.initialized) {
            instance.initialized = true;
            instance.supervisorHost = supervisorHost;
            instance.supervisorPort = supervisorPort;
            instance.autoCommit = autoCommit;
            instance.autoCommitIntervalMs = autoCommitIntervalMs;
            instance.sentReg = false;
            
            Thread thread = new Thread(instance);
            thread.setDaemon(true);
            thread.setName("ConsumerConnector");
            thread.start();
            
            UserLiveCheck checkThread = new UserLiveCheck();
            checkThread.setDaemon(true);
            checkThread.setName("UserLiveCheck");
            checkThread.start();
        } 
    }

    public void commitOffsets() {
        for (Entry<String, List<Fetcher>> e : consumerThreadsMap.entrySet()) {
            String id = e.getKey();
            List<Fetcher> fetches = e.getValue();

            for (Fetcher f : fetches) {
                for (PartitionTopicInfo info : f.getpartitionInfos()) {
                    long lastChanged = info.getConsumedOffsetChanged().get();
                    if (lastChanged == 0) {
                        continue;
                    }
                    long newOffset = info.getConsumedOffset();
                    updateOffset(f.getGroupId(), id, info.topic, info.partition, newOffset);
                    info.updateComsumedOffsetChanged(lastChanged);
                    LOG.debug("Committed " + newOffset + " @" + info.partition + " for topic " + info.topic);
                }
            }
        }
    }
    
    @Override
    public void run() {
        if (autoCommit) {
            LOG.info("starting auto committer every " + autoCommitIntervalMs + " ms");
            scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            scheduler.scheduleAtFixedRate(new AutoCommitTask(), autoCommitIntervalMs, 
                    autoCommitIntervalMs, TimeUnit.MILLISECONDS);
        }
        
        processor = new ConsumerProcessor();
        client = new GenClient(
                processor,
                new ByteBufferNonblockingConnection.ByteBufferNonblockingConnectionFactory(),
                null);
        
        try {
            client.init("consumer", supervisorHost, supervisorPort);
        } catch (ClosedChannelException e) {
            LOG.error(e.getMessage(), e);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }


    /**
     * register consumer data to supervisor
     * @param consumer 
     */
    public void registerConsumer(String topic, String group, String consumerId, Consumer consumer) {
        configMap.put(consumerId, consumer.getConf());
        consumers.put(consumerId, consumer);
        sendRegConsumer(topic, group, consumerId);
        sentReg = true;
    }
    
    public void stop() throws InterruptedException {
        client.shutdown();
        scheduler.shutdown();
        for (List<Fetcher> fetchers : consumerThreadsMap.values()) {
            for (Fetcher f : fetchers) {
                f.shutdown();
            }
        }
        consumerThreadsMap.clear();
        configMap.clear();
        for (Consumer c : consumers.values()) {
            c.shutdown();
        }
        consumers.clear();
    }
    
    public static void shutdownNow() throws InterruptedException {
        instance.stop();
    }
    
    private void sendRegConsumer(String topic, String group, String consumerId) {
        Message message = PBwrap.wrapConsumerReg(group, consumerId, topic);
        LOG.info("register consumer to supervisor " + consumerId + " => " + topic);
        send(message);
    }
    
    private void sendConsumerExit(String topic, String group, String consumerId) {
        Message message = PBwrap.wrapConsumerExit(group, consumerId, topic);
        LOG.info("exit consumer " + consumerId + " => " + topic);
        send(message);
    }
    
    void updateOffset(String groupId, String consumerId, String topic, String partitionName, long offset) {
        Message message = PBwrap.wrapOffsetCommit(groupId, consumerId, topic, partitionName, offset);
        send(message);
    }
    
    private void send(Message message) {
        LOG.debug("send message: " + message);
        Util.send(supervisor, message);
    }
    
    public class ConsumerProcessor implements EntityProcessor<ByteBuffer, ByteBufferNonblockingConnection> {
        private HeartBeat heartbeat = null;
        
        @Override
        public void OnConnected(ByteBufferNonblockingConnection connection) {
            LOG.info("ConsumerConnector connected");
            supervisor = connection;
            heartbeat = new HeartBeat(supervisor, version);
            heartbeat.start();
            if (sentReg) {
                LOG.info("re-register consumers");
                for (Consumer c : consumers.values()) {
                    sendRegConsumer(c.getTopic(), c.getGroup(), c.getConsumerId());
                }
            }
        }

        @Override
        public void OnDisconnected(ByteBufferNonblockingConnection connection) {
            LOG.info("ConsumerConnector disconnected");
            supervisor = null;
            heartbeat.shutdown();
            heartbeat = null;
        }

        @Override
        public void process(ByteBuffer reply, ByteBufferNonblockingConnection from) {
            Message msg = null;
            try {
                msg = PBwrap.Buf2PB(reply);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("received a message can not be deserialized", e);
            }

            LOG.debug("consumer received: " + msg);         
            processInternal(msg);
        }
        
        protected boolean processInternal(Message msg) {
            MessageType type = msg.getType();
            switch (type) {
            case CONSUMER_REG_FAIL:
                ConsumerReg reg = msg.getConsumerReg();
                RetryRegTask retry = new RetryRegTask(reg.getTopic(),
                        reg.getGroupId(), reg.getConsumerId());
                LOG.warn("received ConsumerRegFail for topic " + reg.getTopic() + " ,retry after 5 seconds");
                scheduler.schedule(retry, 5, TimeUnit.SECONDS);
                break;
            case ASSIGN_CONSUMER:
                AssignConsumer assign = msg.getAssignConsumer();
                String groupId = assign.getGroup();
                String consumerId = assign.getConsumerIdString();
                //First, shutdown thread and clear consumer queue.
                List<Fetcher> fetches = consumerThreadsMap.get(consumerId);
                if (fetches != null) {
                    LOG.info("supervisor start to reassign partition, shutdown old FetcherRunnables");
                    for (Fetcher fetcherThread : fetches) {
                        fetcherThread.shutdown();
                    }
                    consumerThreadsMap.remove(consumerId);
                }
                Consumer c = consumers.get(consumerId);
                if (c != null) {
                    c.clearQueue();
                } else {
                    LOG.fatal("unkown consumerId: " + consumerId);
                    return false;
                }
                
                // halt when assigned no partition
                if (assign.getPartitionOffsetsList().isEmpty()) {
                    LOG.info("received no PartitionOffsetsList");
                    return false;
                }

                //create PartitionTopicInfos and group them by broker and comsumerId
                Map<String, List<PartitionTopicInfo>> brokerPartitionInfoMap = new HashMap<String, List<PartitionTopicInfo>>();     
                for (PartitionOffset partitionOffset : assign.getPartitionOffsetsList()) {
                    String topic = assign.getTopic();
                    String brokerString = partitionOffset.getBrokerString();
                    String partitionName = partitionOffset.getPartitionName();
                    long endOffset = partitionOffset.getEndOffset();
                    long committedOffset = partitionOffset.getCommittedOffset();
                    if (committedOffset == MessageAndOffset.UNINITIALIZED_OFFSET || committedOffset == 0) {
                        LOG.info("committed offset UNINITIALIZED or ZERO, replace with end offset " + endOffset);
                        committedOffset = endOffset;
                    }
                    long offset = c.getOffsetStrategy().getOffset(topic, partitionName, endOffset, committedOffset);
                    LOG.info("consume from [" + offset + "] for topic:" + topic + " partition:" + partitionName);
                    PartitionTopicInfo info = 
                            new PartitionTopicInfo(topic, partitionName, brokerString, offset, offset);
                    LOG.debug("create a PartitionTopicInfo: " + info);
                    List<PartitionTopicInfo> partitionList = brokerPartitionInfoMap.get(brokerString);
                    if (partitionList == null) {
                        partitionList = new ArrayList<PartitionTopicInfo>();
                        brokerPartitionInfoMap.put(brokerString, partitionList);
                    }
                    partitionList.add(info);
                }

                //start a fetcher thread for every broker, fetcher thread contains a NIO client
                fetches = new CopyOnWriteArrayList<Fetcher>();
                for (Map.Entry<String, List<PartitionTopicInfo>> entry : brokerPartitionInfoMap.entrySet()) {
                    String brokerString = entry.getKey();
                    List<PartitionTopicInfo> pInfoList = entry.getValue();
                    Fetcher fetcherThread = new Fetcher(groupId, consumerId, brokerString, pInfoList, c.getDataQueue(), configMap.get(consumerId));
                    fetches.add(fetcherThread);
                    fetcherThread.start();
                }
                consumerThreadsMap.put(consumerId, fetches);
                break;
            default:
                LOG.error("Illegal message type " + msg.getType());
            }
            return true;
        }
    }
    
    class AutoCommitTask implements Runnable {

        public void run() {
            try {
                commitOffsets();
            } catch (Throwable e) {
                LOG.error("exception during autoCommit: ", e);
            }
        }
    }
    
    class RetryRegTask implements Runnable {
        private String topic;
        private String group;
        private String consumerId;
        
        public RetryRegTask(String topic, String group, String consumerId) {
            super();
            this.topic = topic;
            this.group = group;
            this.consumerId = consumerId;
        }

        @Override
        public void run() {
            sendRegConsumer(topic, group, consumerId);
        }
    }
    
    class UserLiveCheck extends Thread {
        private static final long DEFUALT_CHECK_PERIOD = 5 * 60 * 1000L;
        @Override
        public void run() {
            LOG.info("start user live check thread with period " + DEFUALT_CHECK_PERIOD + " millisecond");
            while (true) {
                try {
                    Thread.sleep(DEFUALT_CHECK_PERIOD);
                } catch (InterruptedException e) {
                    LOG.error("live check interrupted", e);
                    break;
                }
                try {
                    checkThreadLive();
                } catch (Throwable e) {
                    LOG.error("exception during user live checking: ", e);
                }
            }
        }
        
        private void checkThreadLive() {
            for (Map.Entry<String, Consumer> entry : consumers.entrySet()) {
                String consumerId = entry.getKey();
                Consumer consumer = entry.getValue();
                Thread userThread = consumer.getUserWorkThread();
                if (userThread == null) {
                    continue;
                }
                if (userThread.isAlive()) {
                    LOG.info("user thread: " + userThread.getName() + " for " + consumerId + " is alive");
                    continue;
                } else {
                    LOG.warn("user thread " + userThread.getName() + " has died, clean consumer " + consumerId);
                    consumer.clearQueue();
                    sendConsumerExit(consumer.getTopic(), consumer.getGroup(), consumerId);
                    consumers.remove(consumerId);
                }
            }
        }
    }
}

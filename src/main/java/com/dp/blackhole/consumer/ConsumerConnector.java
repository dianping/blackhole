package com.dp.blackhole.consumer;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.gen.AssignConsumerPB.AssignConsumer;
import com.dp.blackhole.common.gen.AssignConsumerPB.AssignConsumer.PartitionOffset;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.MessagePB.Message.MessageType;
import com.dp.blackhole.node.Node;

public class ConsumerConnector extends Node implements Runnable {
    private static ConsumerConnector instance = new ConsumerConnector();
    
    public static ConsumerConnector getInstance() {
        return instance;
    }
    
    private final Log LOG = LogFactory.getLog(ConsumerConnector.class);
    public volatile boolean initialized = false;
    
    // consumerIdString->[fetcherrunable1, fetcherrunable2]
    private Map<String, List<FetcherRunnable>> consumerThreadsMap;
    //registered consumers
    private ConcurrentHashMap<String, Consumer> consumers;   
    private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);

    private ConsumerConfig config;
    
    private ConsumerConnector() {
    }
    
    public synchronized static void init(ConsumerConfig config) {
        if (!instance.initialized) {
            instance.config = config;
            instance.initialized = true;
            Thread thread = new Thread(instance);
            thread.setDaemon(true);
            thread.start();
        }
    }
    
    public void commitOffsets() {
        for (Entry<String, List<FetcherRunnable>> e : consumerThreadsMap.entrySet()) {
            String id = e.getKey();
            List<FetcherRunnable> fetches = e.getValue();
            for (FetcherRunnable f : fetches) {
                for (PartitionTopicInfo info : f.getpartitionInfos()) {
                    long lastChanged = info.getConsumedOffsetChanged().get();
                    if (lastChanged == 0) {
                        continue;
                    }
                    long newOffset = info.getConsumedOffset();
                    updateOffset(id , info.topic, info.partition, newOffset);
                    info.resetComsumedOffsetChanged(lastChanged);
                    LOG.debug("Committed " + info.partition + " for topic " + info.topic);
                }
            }
        }
    }
    
    @Override
    public void onDisconnected() {
        LOG.info("ConsumerConnector shutting down");
        try {
            scheduler.shutdown();
            for (Map.Entry<String, List<FetcherRunnable>> entry : consumerThreadsMap.entrySet()) {
                for (FetcherRunnable fetcherThread : entry.getValue()) {
                    try {
                        fetcherThread.shutdown();
                    } catch (InterruptedException e) {
                        LOG.warn(e.getMessage(), e);
                    }
                }
                LOG.debug("Clear consumer=>fetcherthread map. KEY[" + entry.getKey() + "]");
            }
            
            for (Consumer c : consumers.values()) {
                c.clearQueue();
                try {
                    c.shutdown();
                } catch (InterruptedException e) {
                    LOG.warn(e.getMessage(), e);
                }
            }
            consumerThreadsMap.clear();
        } catch (Exception e) {
            LOG.error("error during consumer connector shutdown", e);
        }
        LOG.info("ConsumerConnector shut down completed");
    }
    
    @Override
    public void run() {
        consumerThreadsMap = new ConcurrentHashMap<String, List<FetcherRunnable>>();
        consumers = new ConcurrentHashMap<String, Consumer>();
        if (config.isAutoCommit()) {
            int interval = config.getAutoCommitIntervalMs();
            LOG.info("starting auto committer every " + interval + " ms");
            scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            scheduler.scheduleAtFixedRate(new AutoCommitTask(), interval, 
                    interval, TimeUnit.MILLISECONDS);
        }
        try {
            init(config.getSupervisorHost(), config.getSupervisorPort());
        } catch (ClosedChannelException e) {
            LOG.error("Oops, got an exception, thread start fail.", e);
            return;
        } catch (IOException e) {
            LOG.error("Oops, got an exception, thread start fail.", e);
            return;
        }
        super.loop();
    }

    @Override
    protected boolean process(Message msg) {
        MessageType type = msg.getType();
        switch (type) {
        case ASSIGN_CONSUMER:
            AssignConsumer assign = msg.getAssignConsumer();
            
            if (assign.getPartitionOffsetsList().isEmpty()) {
                LOG.info("received no PartitionOffsetsList, retry atfer 3 seconds");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                sendRegConsumer(assign.getTopic(), assign.getGroup(), assign.getConsumerIdString());
                return false;
            }
            
            String consumerId = assign.getConsumerIdString();
            //First, shutdown thread and clear consumer queue.
            List<FetcherRunnable> threadList = consumerThreadsMap.get(consumerId);
            if (threadList != null) {
                for (FetcherRunnable fetcherThread : threadList) {
                    try {
                        fetcherThread.shutdown();
                    } catch (InterruptedException e) {
                        LOG.warn(e.getMessage(), e);
                    }
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

            //create PartitionTopicInfos and group them by broker and comsumerId
            Map<String, List<PartitionTopicInfo>> brokerPartitionInfoMap = new HashMap<String, List<PartitionTopicInfo>>();     
            for (PartitionOffset partitionOffset : assign.getPartitionOffsetsList()) {
                String topic = assign.getTopic();
                String brokerString = partitionOffset.getBrokerString();
                String partitionName = partitionOffset.getPartitionName();
                long offset = partitionOffset.getOffset();
                PartitionTopicInfo info = 
                        new PartitionTopicInfo(topic, partitionName, brokerString, offset, offset);

                List<PartitionTopicInfo> partitionList = brokerPartitionInfoMap.get(brokerString);
                if (partitionList == null) {
                    partitionList = new ArrayList<PartitionTopicInfo>();
                    brokerPartitionInfoMap.put(brokerString, partitionList);
                }
                partitionList.add(info);
            }

            //start a fetcher thread for every broker, fetcher thread contains a NIO client
            threadList = new ArrayList<FetcherRunnable>();
            consumerThreadsMap.put(consumerId, threadList);
            for (Map.Entry<String, List<PartitionTopicInfo>> entry : brokerPartitionInfoMap.entrySet()) {
                String brokerString = entry.getKey();
                List<PartitionTopicInfo> pInfoList = entry.getValue();
                FetcherRunnable fetcherThread = new FetcherRunnable(consumerId, brokerString, pInfoList, c.getDataQueue(), config);
                threadList.add(fetcherThread);
                fetcherThread.start();
            }
            break;
        default:
            LOG.error("Illegal message type " + msg.getType());
        }
        return true;
    }

    /**
     * register consumer data to supervisor
     * @param consumer 
     */
    void registerConsumer(String topic, String group, String consumerId, Consumer consumer) {
        consumers.put(consumerId, consumer);
        sendRegConsumer(topic, group, consumerId);
    }

    private void sendRegConsumer(String topic, String group, String consumerId) {
        Message message = PBwrap.wrapConsumerReg(group, consumerId, topic);
        LOG.info("register consumer to supervisor " + group + "-" + consumerId + " => " + topic);
        super.send(message);
    }
    
    void updateOffset(String consumerId, String topic, String partitionName, long offset) {
        Message message = PBwrap.wrapOffsetCommit(consumerId, topic, partitionName, offset);
        super.send(message);
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
}

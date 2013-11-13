package com.dp.blackhole.consumer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
    
    private final Log LOG = LogFactory.getLog(ConsumerConnector.class);
    
    public static final FetchedDataChunk SHUTDOWN_COMMAND = new FetchedDataChunk(null, null, -1);
    //consumerIdString->[fetcherrunable1, fetcherrunable2]
    private Map<String, List<FetcherRunnable>> consumerThreadsMap;
    //consumerIdString->[info1, info2]
    private Map<String, List<PartitionTopicInfo>> allPartitions;
    //consumerIdString->stream
    Map<String, BlockingQueue<FetchedDataChunk>> queues;
    
    private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
    
    private ConsumerConnector() {}
    
    private static ConsumerConnector instance = new ConsumerConnector();
    
    public static ConsumerConnector getInstance() {
        return instance;
    }
    
    public void commitOffsets() {
        for (Map.Entry<String, List<PartitionTopicInfo>> e : allPartitions.entrySet()) {
            for (PartitionTopicInfo info : e.getValue()) {
                final long lastChanged = info.getConsumedOffsetChanged().get();
                if (lastChanged == 0) {
                    continue;
                }
                final long newOffset = info.getConsumedOffset();
                updateOffset(info.topic, info.partition, newOffset);
                info.resetComsumedOffsetChanged(lastChanged);
                LOG.debug("Committed " + info.partition + " for topic " + info.topic);
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
                entry.getValue().clear();
            }
            
            for (Map.Entry<String, BlockingQueue<FetchedDataChunk>> entry : queues.entrySet()) {
                LOG.debug("Clear consumer=>queue map. KEY[" + entry.getKey() + "]");
                entry.getValue().clear();
                try {
                    entry.getValue().put(SHUTDOWN_COMMAND);
                } catch (InterruptedException e) {
                    LOG.warn(e.getMessage(), e);
                }
            }
            
            for (Map.Entry<String, List<PartitionTopicInfo>> entry : allPartitions.entrySet()) {
                LOG.debug("Clear consumer=>partitions. KEY[" + entry.getKey() + "]");
                entry.getValue().clear();
            }
            
            consumerThreadsMap.clear();
            queues.clear();
            allPartitions.clear();
        } catch (Exception e) {
            LOG.error("error during consumer connector shutdown", e);
        }
        LOG.info("ConsumerConnector shut down completed");
    }
    
    @Override
    public void run() {
        consumerThreadsMap = new HashMap<String, List<FetcherRunnable>>();
        queues = new HashMap<String, BlockingQueue<FetchedDataChunk>>();
        allPartitions = new HashMap<String, List<PartitionTopicInfo>>();
        Properties prop = new Properties();
        try {
            prop.load(new FileReader(new File("consumer.properties")));
        } catch (IOException e) {
            LOG.error("Oops, got an exception, thread start fail.", e);
            return;
        }
        if (Consumer.config.isAutoCommit()) {
            int autoCommitIntervalMs = Consumer.config.getAutoCommitIntervalMs();
            LOG.info("starting auto committer every " + autoCommitIntervalMs + " ms");
            scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            scheduler.scheduleAtFixedRate(new AutoCommitTask(), autoCommitIntervalMs, 
                    autoCommitIntervalMs, TimeUnit.MILLISECONDS);
        }
        try {
            String serverhost = prop.getProperty("supervisor.host");
            int serverport = Integer.parseInt(prop.getProperty("supervisor.port"));
            init(serverhost, serverport);
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
            String consumerIdString = assign.getConsumerIdString();
            //First, shutdown thread and clear queue.
            List<FetcherRunnable> threadList;
            if ((threadList = consumerThreadsMap.get(consumerIdString)) == null) {
                threadList = new ArrayList<FetcherRunnable>();
                consumerThreadsMap.put(consumerIdString, threadList);
            } else {
                for (FetcherRunnable fetcherThread : threadList) {
                    try {
                        fetcherThread.shutdown();
                    } catch (InterruptedException e) {
                        LOG.warn(e.getMessage(), e);
                    }
                }
                threadList.clear();
            }
            BlockingQueue<FetchedDataChunk> queue;
            if ((queue = queues.get(consumerIdString)) == null) {
                LOG.fatal("Queue has been removed unexcepted");
                throw new RuntimeException("Queue has been removed unexcepted");
            }
            queue.clear();
            allPartitions.remove(consumerIdString);

            //Initialize PartitionTopicInfos and group by broker
            List<PartitionTopicInfo> partitionListGroupByConsumer = new ArrayList<PartitionTopicInfo>();
            //brokerString->[partition1, partition2]
            Map<String, List<PartitionTopicInfo>> brokerPartitionInfoMap = new HashMap<String, List<PartitionTopicInfo>>();
            List<PartitionTopicInfo> partitionListGroupByBroker;
            List<PartitionOffset> offsets = assign.getPartitionOffsetsList();
            for (PartitionOffset partitionOffset : offsets) {
                String brokerString = partitionOffset.getBrokerString();
                String partitionName = partitionOffset.getPartitionName();
                long offset = partitionOffset.getOffset();
                LOG.info(assign.getTopic() + "  " + consumerIdString + " ==> " + brokerString + " -> " + partitionName + " claimming");
                PartitionTopicInfo info = new PartitionTopicInfo(assign.getTopic(),partitionName, 
                        brokerString, queue, offset, offset);
                if ((partitionListGroupByBroker = brokerPartitionInfoMap.get(brokerString)) == null) {
                    partitionListGroupByBroker = new ArrayList<PartitionTopicInfo>();
                    brokerPartitionInfoMap.put(brokerString, partitionListGroupByBroker);
                }
                partitionListGroupByBroker.add(info);
                partitionListGroupByConsumer.add(info);
            }
            allPartitions.put(consumerIdString, partitionListGroupByConsumer);

            //start a fetcher thread for every broker, fetcher thread contains a NIO client
            for (Map.Entry<String, List<PartitionTopicInfo>> entry : brokerPartitionInfoMap.entrySet()) {
                FetcherRunnable fetcherThread = new FetcherRunnable(entry.getKey(), entry.getValue());
                threadList.add(fetcherThread);
                fetcherThread.start();
            }
            break;
        default:
            LOG.error("Illegal message type " + msg.getType());
        }
        return false;
    }

    /**
     * register consumer data to supervisor
     */
    void registerConsumer(final String consumerIdString, final String topic,
            final int minConsumersInGroup) {
        BlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<FetchedDataChunk>(Consumer.config.getMaxQueuedChunks());
        synchronized (this) {
            queues.put(consumerIdString, queue);
        }
        Message message = PBwrap.wrapConsumerReg(consumerIdString, topic, minConsumersInGroup);
        LOG.info("register consumer to supervisor " + consumerIdString + " => " + topic);
        super.send(message);
    }
    
    void updateOffset(String topic, String partitionName, long offset) {
        Message message = PBwrap.wrapOffsetCommit(topic, partitionName, offset);
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

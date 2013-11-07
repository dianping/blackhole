package com.dp.blackhole.consumer;

import static java.lang.String.format;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.collectornode.persistent.protocol.OffsetRequest;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.gen.AssignConsumerPB.AssignConsumer;
import com.dp.blackhole.common.gen.AssignConsumerPB.AssignConsumer.PartitionOffset;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.MessagePB.Message.MessageType;
import com.dp.blackhole.consumer.exception.InvalidConfigException;
import com.dp.blackhole.node.Node;

public class ConsumerConnector extends Node implements Runnable {
    
    private final Log LOG = LogFactory.getLog(ConsumerConnector.class);
    
    public static final FetchedDataChunk SHUTDOWN_COMMAND = new FetchedDataChunk(null, null, -1);
    //consumerIdString->[fetcherrunable1, fetcherrunable2]
    private Map<String, List<FetcherRunnable>> consumerThreadsMap;
    //topic->[consumerIdString1] in this JVM
    private Map<String, String> topicToConsumerMap;
    //consumerIdString->(partitionName->info(chunkqueue))
    private Map<String, Map<String, PartitionTopicInfo>> allPartitions;
    //consumerIdString->stream
    Map<String, BlockingQueue<FetchedDataChunk>> queues;//TODO SAFT-VERVIFY
    
    private final Scheduler scheduler = new Scheduler(1, "consumer-autocommit-", false);
    
    private ConsumerConnector() {}
    
    private static ConsumerConnector instance = new ConsumerConnector();
    
    public static ConsumerConnector getInstance() {
        return instance;
    }
    
    public void commitOffsets() {
        for (Map.Entry<String, Map<String, PartitionTopicInfo>> e : allPartitions.entrySet()) {
            for (PartitionTopicInfo info : e.getValue().values()) {
                final long lastChanged = info.getConsumedOffsetChanged().get();
                if (lastChanged == 0) {
                    LOG.trace("consume offset not changed");
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
            
            if (Consumer.config.isAutoCommit()) {
                commitOffsets();
            }
            
            for (Map.Entry<String, Map<String, PartitionTopicInfo>> entry : allPartitions.entrySet()) {
                LOG.debug("Clear consumer=>partition map. KEY[" + entry.getKey() + "]");
                entry.getValue().clear();
            }
            
            consumerThreadsMap.clear();
            queues.clear();
            topicToConsumerMap.clear();
            allPartitions.clear();
        } catch (Exception e) {
            LOG.error("error during consumer connector shutdown", e);
        }
        LOG.info("ConsumerConnector shut down completed");
    }
    
    @Override
    public void run() {
        consumerThreadsMap = new HashMap<String, List<FetcherRunnable>>();
        topicToConsumerMap = new HashMap<String, String>();
        queues = new HashMap<String, BlockingQueue<FetchedDataChunk>>();
        allPartitions = new ConcurrentHashMap<String, Map<String, PartitionTopicInfo>>();
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
            scheduler.scheduleWithRate(new AutoCommitTask(), autoCommitIntervalMs, autoCommitIntervalMs);
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
            String consumerIdString = topicToConsumerMap.get(assign.getTopic());
            if (consumerIdString.equals(assign.getConsumerIdString())) {
                BlockingQueue<FetchedDataChunk> queue;
                if ((queue = queues.get(consumerIdString)) == null) {
                    LOG.fatal("Queue has been removed unexcepted");
                    throw new RuntimeException("Queue has been removed unexcepted");
                }
                //CLEAR & COLSE THREAD
                queue.clear();
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
                
                //brokerString->[partition1, partition2]
                Map<String, List<PartitionTopicInfo>> brokerPartitionInfoMap = new HashMap<String, List<PartitionTopicInfo>>();

                List<PartitionOffset> offsets = assign.getPartitionOffsetsList();
                for (PartitionOffset partitionOffset : offsets) {
                    String brokerString = partitionOffset.getBrokerString();
                    String partitionName = partitionOffset.getPartitionName();
                    long offset = partitionOffset.getOffset();
                    //Offset from supervisor less than zero means that it should be correct first. That's a specification rule
                    if (offset < 0) {
                        if (OffsetRequest.SMALLES_TIME_STRING.equals(Consumer.config.getAutoOffsetReset())) {
                            offset = earliestOrLatestOffset(assign.getTopic(), brokerString, partitionName,
                                    OffsetRequest.EARLIES_TTIME);
                        } else if (OffsetRequest.LARGEST_TIME_STRING.equals(Consumer.config.getAutoOffsetReset())) {
                            offset = earliestOrLatestOffset(assign.getTopic(), brokerString, partitionName,
                                    OffsetRequest.LATES_TTIME);
                        } else {
                            throw new InvalidConfigException("Wrong value in autoOffsetReset in ConsumerConfig");
                        }

                    }
                    LOG.info(assign.getTopic() + "  " + consumerIdString + " ==> " + brokerString + " > " + partitionName + " claimming");
                    AtomicLong consumedOffset = new AtomicLong(offset);
                    AtomicLong fetchedOffset = new AtomicLong(offset);
                    PartitionTopicInfo info = new PartitionTopicInfo(assign.getTopic(),//
                            partitionName,//
                            brokerString,//
                            queue,//
                            consumedOffset,//
                            fetchedOffset);//
                    Map<String, PartitionTopicInfo> ppMap = new ConcurrentHashMap<String, PartitionTopicInfo>();
                    ppMap.put(partitionName, info);
                    allPartitions.put(consumerIdString, ppMap);//replaced value when re-assign
                    LOG.info(info + " selected new offset " + offset);
                    
                    List<PartitionTopicInfo> list = brokerPartitionInfoMap.get(brokerString);
                    if (list == null) {
                        list = new ArrayList<PartitionTopicInfo>();
                        brokerPartitionInfoMap.put(brokerString, list);
                    }
                    list.add(info);
                }

                //start a fetcher thread for every broker, fetcher thread contains a NIO client
                for (Map.Entry<String, List<PartitionTopicInfo>> entry : brokerPartitionInfoMap.entrySet()) {
                    FetcherRunnable fetcherThread = new FetcherRunnable(entry.getKey(), entry.getValue());
                    threadList.add(fetcherThread);
                    fetcherThread.start();
                }
            }
            break;
        default:
            LOG.error("Illegal message type " + msg.getType());
        }
        return false;
    }

    private long earliestOrLatestOffset(String topic, String brokerString,
            String partitionName, long earliestOrLatest) {
        SimpleConsumer simpleConsumer = null;
        long producedOffset = -1;

        try {
            simpleConsumer = new SimpleConsumer(Util.getHostFromBroker(brokerString), Util.getPortFromBroker(brokerString));
            long offset = simpleConsumer.getOffsetsBefore(topic, partitionName, earliestOrLatest, 1);
            if (offset > 0) {
                producedOffset = offset;
            }
        } catch (Exception e) {
            LOG.error("error in earliestOrLatestOffset() ", e);
        } finally {
            if (simpleConsumer != null) {
                try {
                    simpleConsumer.close();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
        return producedOffset;
    }

    /**
     * register consumer data to supervisor
     */
    void registerConsumer(final String consumerIdString, final String topic,
            final int minConsumersInGroup) {
        BlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<FetchedDataChunk>(Consumer.config.getMaxQueuedChunks());
        synchronized (this) {
            //just one consumerIdString exists to one topic in a JVM
            topicToConsumerMap.put(topic, consumerIdString);
            queues.put(consumerIdString, queue);
        }
        Message message = PBwrap.wrapConsumerReg(consumerIdString, topic, minConsumersInGroup);
        LOG.info(format("register consumer to supervisor [%s] => [%s]", consumerIdString, topic));
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
    
    class Scheduler {

        final Log LOG = LogFactory.getLog(Scheduler.class);
        
        final AtomicLong threadId = new AtomicLong(0);

        final ScheduledThreadPoolExecutor executor;

        final String baseThreadName;

        public Scheduler(int numThreads, final String baseThreadName, final boolean isDaemon) {
            this.baseThreadName = baseThreadName;
            executor = new ScheduledThreadPoolExecutor(numThreads, new ThreadFactory() {

                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, baseThreadName + threadId.getAndIncrement());
                    t.setDaemon(isDaemon);
                    return t;
                }
            });
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }

        public ScheduledFuture<?> scheduleWithRate(Runnable command, long delayMs, long periodMs) {
            return executor.scheduleAtFixedRate(command, delayMs, periodMs, TimeUnit.MILLISECONDS);
        }

        public void shutdownNow() {
            executor.shutdownNow();
            LOG.info("ShutdownNow scheduler " + baseThreadName + " with " + threadId.get() + " threads.");
        }

        public void shutdown() {
            executor.shutdown();
            LOG.info("Shutdown scheduler " + baseThreadName + " with " + threadId.get() + " threads.");
        }
    }
}

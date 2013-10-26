package com.dp.blackhole.consumer;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.appnode.LogReader;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.gen.AssignCollectorPB.AssignCollector;
import com.dp.blackhole.common.gen.AssignConsumerPB.AssignConsumer;
import com.dp.blackhole.common.gen.AssignConsumerPB.AssignConsumer.Broker;
import com.dp.blackhole.common.gen.AssignConsumerPB.AssignConsumer.Pair;
import com.dp.blackhole.common.gen.AssignConsumerPB.AssignConsumer.PartitionOffset;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.MessagePB.Message.MessageType;
import com.dp.blackhole.node.Node;

public class ConsumerConnector extends Node implements Runnable {
    
    private final Log LOG = LogFactory.getLog(ConsumerConnector.class);
    //consumerIdString->fetchthread?or fetcherrunable>
    private Map<String, FetcherRunnable> fetcherThreads;//TODO SAFT-VERVIFY
    //topic->[consumerIdString1,consumerIdString2] in this JVM
    private Map<String, Set<String>> topicConsumersMap;//TODO SAFT-VERVIFY
    //consumerIdString1->stream
    Map<String, BlockingQueue<FetchedDataChunk>> queues;//TODO SAFT-VERVIFY
    
    private ConsumerConnector() {}
    
    private static ConsumerConnector instance = new ConsumerConnector();
    
    public static ConsumerConnector getInstance() {
        return instance;
    }
    
    @Override
    public void run() {
        fetcherThreads = new HashMap<String, FetcherRunnable>();
        topicConsumersMap = new HashMap<String, Set<String>>();
        queues = new HashMap<String, BlockingQueue<FetchedDataChunk>>();
        super.loop();
    }

    public synchronized void register(String consumerIdString, FetcherRunnable fetchThread) {
        fetcherThreads.put(consumerIdString, fetchThread);
    }
    
    public synchronized void unregister(String consumerIdString) {
        if (fetcherThreads.remove(consumerIdString) == null) {
            LOG.error("There is no fetch thead for consumerId " + consumerIdString);
        }
    }
    
    @Override
    protected boolean process(Message msg) {
        MessageType type = msg.getType();
        switch (type) {
        case ASSIGN_CONSUMER:
            AssignConsumer assign = msg.getAssignConsumer();
            if (topicConsumersMap.containsKey(assign.getTopic())) {
                List<Pair> pairs = assign.getPairsList();
                Map<Partition, PartitionTopicInfo> currentRegistry = new HashMap<Partition, PartitionTopicInfo>();
                
                for (Pair pair : pairs) {
                    String consumerIdString = pair.getConsumerIdString();
                    if (!topicConsumersMap.get(assign.getTopic()).contains(consumerIdString)) {
                        break;//break because consumer from supervisor maybe registered in other JVM
                    }
                    BlockingQueue<FetchedDataChunk> queue;
                    if ((queue = queues.get(consumerIdString)) == null) {//TODO Does it need replace?
                        queue = new LinkedBlockingQueue<FetchedDataChunk>();
                        queues.put(consumerIdString, queue);
                    }
                    
                    Map<Broker, List<PartitionTopicInfo>> m = new HashMap<Broker, List<PartitionTopicInfo>>();
                    
                    List<PartitionOffset> offsets = pair.getPartitionOffsetsList();
                    for (PartitionOffset partitionOffset : offsets) {
                        String brokerPartition = partitionOffset.getBrokerPartition();
                        Partition partition = Partition.parse(brokerPartition);
                        long offset = partitionOffset.getOffset();
                        LOG.info("[" + assign.getTopic() + "]    " + consumerIdString + " ==> " + brokerPartition + " claimming");
                        AtomicLong consumedOffset = new AtomicLong(offset);
                        AtomicLong fetchedOffset = new AtomicLong(offset);
                        PartitionTopicInfo info = new PartitionTopicInfo(assign.getTopic(),//
                                partition,//
                                queue,//
                                consumedOffset,//
                                fetchedOffset);//
                        currentRegistry.put(partition, info);
                        LOG.info(info + " selected new offset " + offset);
                        
                        Broker ownerBroker = partitionOffset.getOwnerBroker();
                        List<PartitionTopicInfo> list = m.get(ownerBroker);
                        if (list == null) {
                            list = new ArrayList<PartitionTopicInfo>();
                            m.put(ownerBroker, list);
                        }
                        list.add(info);
                    }
                    //start a fetcher thread for every broker, fetcher thread contains a NIO client
                    for (Map.Entry<Broker, List<PartitionTopicInfo>> entry : m.entrySet()) {
                        FetcherRunnable fetcherThread = new FetcherRunnable(entry.getKey(), entry.getValue());
                        fetcherThreads.put(consumerIdString, fetcherThread);
                        fetcherThread.start();
                    }
                }
            }
            break;
        default:
            LOG.error("Illegal message type " + msg.getType());
        }
        return false;
    }

    /**
     * 
     * @return topic->(consumerIdString-0,consumerIdString-1..)
     */
    public Map<String, Set<String>> getConsumerPerTopic(String topic, List<Pair> pairsList) {
        Map<String, Set<String>> consumerThreadIdsPerTopicMap = new HashMap<String, Set<String>>();
        for (Pair pair : pairsList) {
            Set<String> consumerSet = new HashSet<String>();
            final int nCounsumers = pairsList.size();
            for (int i = 0; i < nCounsumers; i++) {
                consumerSet.add(pair.getConsumerIdString());
            }
            consumerThreadIdsPerTopicMap.put(topic, consumerSet);
        }
        return consumerThreadIdsPerTopicMap;
    }

    /**
     * register consumer data to supervisor
     */
    void registerConsumer(final String consumerIdString, final String topic,
            final int minConsumersInGroup) {
        Set<String> consumerSet;
        synchronized (this) {
            if ((consumerSet = topicConsumersMap.get(topic)) == null) {
                consumerSet = new HashSet<String>();//TODO safety needed?
                topicConsumersMap.put(topic, consumerSet);
            }
            consumerSet.add(consumerIdString);
        }
        Message message = PBwrap.wrapConsumerReg(consumerIdString, topic, minConsumersInGroup);
        LOG.info(format("register consumer to supervisor [%s] => [%s]", consumerIdString, topic));
        super.send(message);
    }
}

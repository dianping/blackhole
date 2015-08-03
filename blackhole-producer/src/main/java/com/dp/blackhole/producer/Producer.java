package com.dp.blackhole.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.DaemonThreadFactory;
import com.dp.blackhole.common.TopicCommonMeta;
import com.dp.blackhole.common.Util;

public class Producer {
    private static final Log LOG = LogFactory.getLog(Producer.class);
    private enum PartitionConnectionState {UNASSIGNED, ASSIGNED, STOPPED}
    private final String topic;
    private String producerId;
    private TopicCommonMeta topicMeta;
    private Map<String, PartitionConnection> partitionConnectionMap;
    private ConcurrentHashMap<String, AtomicReference<PartitionConnectionState>> partitionStateMap;
    private ProducerConnector connector;
    private int currentPartitionIndex;
    private ScheduledExecutorService service;
    
    public Producer(String topic) {
        this.topic = topic;
        this.connector = ProducerConnector.getInstance();
        if (!this.connector.initialized) {
            this.connector.init();
        }
        this.partitionConnectionMap = new ConcurrentHashMap<String, PartitionConnection>();
        this.partitionStateMap = new ConcurrentHashMap<String, AtomicReference<PartitionConnectionState>>();
        this.currentPartitionIndex = 0;
        this.service = Executors.newSingleThreadScheduledExecutor(
                new DaemonThreadFactory("ProducerRollTask-" + topic));
    }
    
    public String getTopic() {
        return topic;
    }

    public String getProducerId() {
        return producerId;
    }

    void setProducerId(String producerId) {
        this.producerId = producerId;
    }
    
    public TopicCommonMeta geTopicMeta() {
        return topicMeta;
    }

    void setTopicMeta(TopicCommonMeta topicMeta) {
        this.topicMeta = topicMeta;
        initRollTask();
    }

    private void initRollTask() {
        long currentTs = Util.getTS();
        long delay = Util.getNextRollTs(currentTs, topicMeta.getRollPeriod()) - currentTs + 100L;
        this.service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for (PartitionConnection conn : partitionConnectionMap.values()) {
                    try {
                        conn.sendRollRequest();
                    } catch (IOException e) {
                        reassignPartitionConnection(conn.getPartitionId());
                    }
                }
            }
        }, delay, topicMeta.getRollPeriod() * 1000L, TimeUnit.MILLISECONDS);
    }

    public void register() {
        connector.prepare(this);
    }
    
    /**
     * 
     * @param message String
     * @return true if the message sent successfully, or false
     */
    public boolean sendMessage(String message) {
        return sendMessage(message.getBytes());
    }
    
    /**
     * 
     * @param message byte[]
     * @return true if the message sent successfully, or false
     */
    public boolean sendMessage(byte[] message) {
        String partitionId = getPartitionIdRoundRobin();
        if (partitionId == null) {
            return false;
        }
        try {
            partitionConnectionMap.get(partitionId).cacahAndSendLine(message);
        } catch (IOException e) {
            reassignPartitionConnection(partitionId);
            return false;
        }
        return true;
    }
    
    public void closeProducer() {
        for (PartitionConnection conn : partitionConnectionMap.values()) {
            AtomicReference<PartitionConnectionState> current = partitionStateMap.get(conn.getPartitionId());
            current.set(PartitionConnectionState.STOPPED);
            conn.close();
            LOG.info("close producer " + producerId + " partition " + conn.getPartitionId() + " disconnected.");
        }
        partitionConnectionMap.clear();
        partitionStateMap.clear();
        currentPartitionIndex = 0;
    }
    
    private String getPartitionIdRoundRobin() {
        int total = partitionConnectionMap.size();
        if (total == 0) {
            return null;
        }
        int index = (currentPartitionIndex++) % total;
        ArrayList<String> array = new ArrayList<String>(partitionConnectionMap.keySet());
        return array.get(index);
    }

    public void assignPartitionConnection(PartitionConnection partitionConnection) {
        String partitionId = partitionConnection.getPartitionId();
        partitionConnectionMap.put(partitionId, partitionConnection);
        AtomicReference<PartitionConnectionState> oldState = partitionStateMap.putIfAbsent(
                partitionId,
                new AtomicReference<PartitionConnectionState>(
                        PartitionConnectionState.ASSIGNED));
        if (oldState == null) {
            LOG.info("Assign a new parition connection for " + partitionId + " -> " + PartitionConnectionState.ASSIGNED.name());
        } else {
            LOG.error("partition connection has been assigned.");
        }
    }
    
    private void reassignPartitionConnection(String partitionId) {
        AtomicReference<PartitionConnectionState> currentState = partitionStateMap.get(partitionId);
        if (currentState.compareAndSet(PartitionConnectionState.ASSIGNED, PartitionConnectionState.UNASSIGNED)) {
            PartitionConnection partitionConnection = partitionConnectionMap.get(partitionId);
            int reassignDelay = partitionConnection.getReassignDelaySeconds();
            connector.getStreamHealthChecker().unregister(partitionConnection);
            partitionConnection.close();
            partitionConnectionMap.remove(partitionId);
            //must unregister from ConnectionChecker before re-assign
            connector.reportPartitionConnectionFailure(topic, producerId, partitionId, Util.getTS(), reassignDelay);
        }
    }
}

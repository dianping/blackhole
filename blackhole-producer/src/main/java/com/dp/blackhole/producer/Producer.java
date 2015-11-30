package com.dp.blackhole.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
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
    private Map<PartitionConnection, String> partitionConnectionMap;
    private ConcurrentHashMap<String, AtomicReference<PartitionConnectionState>> partitionStateMap;
    private ProducerConnector connector;
    private int currentPartitionIndex;
    private ScheduledExecutorService service;
    
    public Producer(String topic) {
        this(topic, null);
    }
    
    public Producer(String topic, Properties prop) {
        this.topic = topic;
        this.connector = ProducerConnector.getInstance();
        this.connector.init(prop);
        this.partitionConnectionMap = new ConcurrentHashMap<PartitionConnection, String>();
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
                for (Map.Entry<PartitionConnection, String> entry : partitionConnectionMap.entrySet()) {
                    PartitionConnection conn = entry.getKey();
                    String partitionId = entry.getValue();
                    try {
                        conn.sendRollRequest();
                    } catch (IOException e) {
                        LOG.warn("send roll request fail for " + partitionId, e);
                        reassignPartitionConnection(conn);
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
        PartitionConnection partitionConnection = getPartitionIdRoundRobin();
        if (partitionConnection == null) {
            return false;
        }
        try {
            partitionConnection.cacahAndSendLine(message);
        } catch (IOException e) {
            reassignPartitionConnection(partitionConnection);
            return false;
        }
        return true;
    }
    
    /**
     * force flush to socket if there are some context in buffer
     */
    public void flush() {
        for (Map.Entry<PartitionConnection, String> entry : partitionConnectionMap.entrySet()) {
            PartitionConnection conn = entry.getKey();
            try {
                conn.sendMessage();
            } catch (IOException e) {
                reassignPartitionConnection(conn);
            }
        }
    }
    
    public void closeProducer() {
        for (Map.Entry<PartitionConnection, String> entry : partitionConnectionMap.entrySet()) {
            PartitionConnection conn = entry.getKey();
            String partitionId = entry.getValue();
            AtomicReference<PartitionConnectionState> current = partitionStateMap.get(partitionId);
            current.set(PartitionConnectionState.STOPPED);
            conn.close();
            LOG.info("close producer " + producerId + " partition " + partitionId + " disconnected.");
        }
        partitionConnectionMap.clear();
        partitionStateMap.clear();
        currentPartitionIndex = 0;
    }
    
    private PartitionConnection getPartitionIdRoundRobin() {
        ArrayList<PartitionConnection> connections = new ArrayList<PartitionConnection>(partitionConnectionMap.keySet());
        if (connections.isEmpty()) {
            return null;
        }
        int index = (currentPartitionIndex++) % connections.size();
        return connections.get(index);
    }

    void assignPartitionConnection(PartitionConnection partitionConnection) {
        String partitionId = partitionConnection.getPartitionId();
        partitionConnectionMap.put(partitionConnection, partitionId);
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
    
    private void reassignPartitionConnection(PartitionConnection partitionConnection) {
        String partitionId = partitionConnection.getPartitionId();
        partitionConnection.close();
        AtomicReference<PartitionConnectionState> currentState = partitionStateMap.get(partitionId);
        if (currentState != null && currentState.compareAndSet(PartitionConnectionState.ASSIGNED, PartitionConnectionState.UNASSIGNED)) {
            int reassignDelay = partitionConnection.getReassignDelaySeconds();
            connector.getStreamHealthChecker().unregister(partitionConnection);
            partitionConnectionMap.remove(partitionConnection);
            //must unregister from ConnectionChecker before re-assign
            connector.reportPartitionConnectionFailure(topic, producerId, partitionId, Util.getTS(), reassignDelay);
        }
    }
}

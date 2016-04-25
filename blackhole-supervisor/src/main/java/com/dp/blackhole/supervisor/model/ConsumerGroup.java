package com.dp.blackhole.supervisor.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.storage.MessageAndOffset;
import com.dp.blackhole.supervisor.Supervisor;

public class ConsumerGroup {
    private ConsumerGroupKey groupKey;
    private Map<String, AtomicLong> committedOffsetMap; //partition -> committed offset
    private List<ConsumerDesc> consumes;
    
    public static final Log LOG = LogFactory.getLog(Supervisor.class);
    
    public ConsumerGroup(ConsumerGroupKey groupKey) {
        this.groupKey = groupKey;
        committedOffsetMap = new ConcurrentHashMap<String, AtomicLong>();
    }

    public String getTopic() {
        return groupKey.getTopic();
    }
    
    public String getGroupId() {
        return groupKey.getGroupId();
    }
    
    public synchronized boolean exists(ConsumerDesc consumer) {
        return consumes == null ? false : consumes.contains(consumer);
    }

    public Map<String, AtomicLong> getCommitedOffsets() {
        return committedOffsetMap;
    }
    
    public synchronized void setConsumers(List<ConsumerDesc> consumes) {
        this.consumes = consumes;
    }
    
    public synchronized List<ConsumerDesc> getConsumes() {
        return consumes;
    }

    public synchronized void unregisterConsumer(ConsumerDesc consumer) {
        if (consumes != null) {
            consumes.remove(consumer);
        }
    }

    public synchronized int getConsumerCount() {
        return consumes == null ? 0 : consumes.size();
    }

    public void updateOffset(String consumerId, String topic, String partition, long offset) {
        AtomicLong committedOffset = committedOffsetMap.get(partition);
        if (committedOffset == null) {
            LOG.error("can not find PartitionInfo by partition: " + "[" + topic +"]" + partition + " ,request from " + consumerId);
        } else {
            committedOffset.set(offset);
        }
    }
    
    public long getCommittedOffsetByPartitionId(String partitionId) {
        AtomicLong committedOffset = committedOffsetMap.get(partitionId);
        if (committedOffset != null) {
            return committedOffset.get();
        } else {
            return MessageAndOffset.UNINITIALIZED_OFFSET;
        }
    }
    
    public void resetCommittedOffsetByPartitionId(String partitionId) {
        AtomicLong committedOffset = committedOffsetMap.get(partitionId);
        if (committedOffset != null) {
            committedOffset.getAndSet(MessageAndOffset.UNINITIALIZED_OFFSET);
        }
    }

    public void update(List<ConsumerDesc> consumes,
            ArrayList<ArrayList<PartitionInfo>> assignPartitions,
            ArrayList<PartitionInfo> partitions) {
        for (int i = 0; i < consumes.size(); i++) {
            ConsumerDesc cond = consumes.get(i);
            cond.setPartitions(assignPartitions.get(i));
        }
        setConsumers(consumes);
        for (PartitionInfo pinfo : partitions) {
            String id = pinfo.getId();
            if (committedOffsetMap.get(id) == null) {
                committedOffsetMap.put(pinfo.getId(), new AtomicLong(MessageAndOffset.UNINITIALIZED_OFFSET));
            }
        }
    }
}

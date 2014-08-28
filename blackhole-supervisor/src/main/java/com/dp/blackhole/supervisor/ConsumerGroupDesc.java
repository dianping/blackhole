package com.dp.blackhole.supervisor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConsumerGroupDesc {
    private ConsumerGroup group;
    private Map<String, AtomicLong> commitedOffsetMap;
    private Map<ConsumerDesc, ArrayList<String>> consumeMap;
    
    public static final Log LOG = LogFactory.getLog(Supervisor.class);
    
    public ConsumerGroupDesc(ConsumerGroup group) {
        this.group = group;
        commitedOffsetMap = new ConcurrentHashMap<String, AtomicLong>();
        consumeMap = new ConcurrentHashMap<ConsumerDesc, ArrayList<String>>();
    }

    public String getTopic () {
        return group.getTopic();
    }
    
    public String getGroupId () {
        return group.getGroupId();
    }
    
    public boolean exists (ConsumerDesc consumer) {
        return consumeMap.containsKey(consumer);
    }

    public Map<String, AtomicLong> getCommitedOffsets () {
        return commitedOffsetMap;
    }
    
    public ArrayList<ConsumerDesc> getConsumes () {
        return new ArrayList<ConsumerDesc>(consumeMap.keySet());
    }

    public void unregisterConsumer(ConsumerDesc consumer) {
        consumeMap.remove(consumer);
    }

    public Collection<ConsumerDesc> getConsumers() {
        return consumeMap.keySet();
    }

    public void updateOffset(String consumerId, String topic, String partition, long offset) {
        AtomicLong commitedOffset = commitedOffsetMap.get(partition);
        if (commitedOffset == null) {
            LOG.error("can not find PartitionInfo by partition: " + "[" + topic +"]" + partition + " ,request from " + consumerId);
        } else {
            commitedOffset.set(offset);
        }
    }

    public void update(ArrayList<ConsumerDesc> consumes,
            ArrayList<ArrayList<PartitionInfo>> assignPartitions,
            ArrayList<PartitionInfo> partitions) {
        consumeMap.clear();
        int index = 0;
        for (ConsumerDesc cond : consumes) {
            ArrayList<String> ids = new ArrayList<String>();
            for (PartitionInfo pinfo : assignPartitions.get(index)) {
                ids.add(pinfo.getId());
            }
            consumeMap.put(cond, ids);
            index++;
        }
        for (PartitionInfo pinfo : partitions) {
            String id = pinfo.getId();
            if (commitedOffsetMap.get(id) == null) {
                commitedOffsetMap.put(pinfo.getId(), new AtomicLong(0));
            }
        }
    }
    
}

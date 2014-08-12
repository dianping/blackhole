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
    // currrently available partitions
    private Map<String, AtomicLong> commitedOffsetMap;
    private Map<ConsumerDesc, ArrayList<String>> consumeMap;
    
    public static final Log LOG = LogFactory.getLog(Supervisor.class);
    
    public ConsumerGroupDesc(ConsumerGroup group, Collection<PartitionInfo> pinfos) {
        this.group = group;
        commitedOffsetMap = new ConcurrentHashMap<String, AtomicLong>();
        for (PartitionInfo pinfo: pinfos) {
            commitedOffsetMap.put(pinfo.getId(), new AtomicLong(0));
        }
        consumeMap = new ConcurrentHashMap<ConsumerDesc, ArrayList<String>>();
    }

    public boolean exists (ConsumerDesc consumer) {
        return consumeMap.containsKey(consumer);
    }

    public Map<String, AtomicLong> getCommitedOffsets () {
        return commitedOffsetMap;
    }
    
    public Map<ConsumerDesc, ArrayList<String>> getConsumeMap () {
        return consumeMap;
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
}

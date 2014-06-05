package com.dp.blackhole.supervisor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConsumerGroupDesc {
    private ConsumerGroup group;
    // currrently available partitions
    private Map<String, PartitionInfo> partitions;
    private Map<ConsumerDesc, ArrayList<String>> consumeMap;
    
    public static final Log LOG = LogFactory.getLog(Supervisor.class);
    
    public ConsumerGroupDesc(ConsumerGroup group, Collection<PartitionInfo> pinfos) {
        this.group = group;
        partitions = Collections.synchronizedMap(new HashMap<String, PartitionInfo>());
        for (PartitionInfo pinfo: pinfos) {
            partitions.put(pinfo.getId(), new PartitionInfo(pinfo));
        }
        consumeMap = Collections.synchronizedMap(new HashMap<ConsumerDesc, ArrayList<String>>());
    }

    public boolean exists (ConsumerDesc consumer) {
        return consumeMap.containsKey(consumer);
    }

    public Map<String, PartitionInfo> getPartitions () {
        return partitions;
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
        PartitionInfo info = partitions.get(partition);
        if (info == null) {
            LOG.error("can not find PartitionInfo by partition: " + "[" + topic +"]" + partition + " ,request from " + consumerId);
        } else {
            info.setEndOffset(offset);
        }
    }
    
    // TODO
    @Override
    public String toString() {
        return super.toString();
    }
    
}

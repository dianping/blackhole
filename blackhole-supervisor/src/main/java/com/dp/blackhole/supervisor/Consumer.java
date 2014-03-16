package com.dp.blackhole.supervisor;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.SimpleConnection;

public class Consumer {
    public static final Log LOG = LogFactory.getLog(Consumer.class);
    
    private String id;
    private String topic;
    private ConsumerGroup group;
    private SimpleConnection connection;
    private long reg_ts;
    
    private ConcurrentHashMap<String, PartitionInfo> consumedPartitions;
    
    public Consumer(String id, ConsumerGroup group, String topic, SimpleConnection from) {
        this.id = id;
        this.group = group;
        this.topic = topic;
        connection = from;
        reg_ts = Util.getTS();
        consumedPartitions = new ConcurrentHashMap<String, PartitionInfo>();
    }

    public String getId() {
        return id;
    }
    
    public SimpleConnection getConnection() {
        return connection;
    }

    public ConsumerGroup getConsumerGroup() {
        return group;
    }

    public void setPartitions(ArrayList<PartitionInfo> arrayList) {
        for (PartitionInfo info : arrayList) {
            PartitionInfo pinfo = new PartitionInfo(info);
            consumedPartitions.put(pinfo.getId(), pinfo);
        }
    }

    public void updateOffset(String id, long offset) {
        PartitionInfo info = consumedPartitions.get(id);
        if (info == null) {
            LOG.error("can not find partitionInfo by id " + id);
            return;
        }
        info.setEndOffset(offset);
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("id: ")
            .append(id)
            .append(" group: ")
            .append(group)
            .append(" topic ")
            .append(topic);
        return builder.toString();
            
    }

}

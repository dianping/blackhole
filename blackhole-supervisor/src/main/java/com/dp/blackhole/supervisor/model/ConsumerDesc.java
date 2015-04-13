package com.dp.blackhole.supervisor.model;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;

import com.dp.blackhole.network.SimpleConnection;

public class ConsumerDesc extends NodeDesc {
    public static final Log LOG = LogFactory.getLog(ConsumerDesc.class);
    
    private String topic;
    private String groupId;
    private SimpleConnection from;
    private List<PartitionInfo> partitions;
    
    public ConsumerDesc(String consumerId, String groupId, String topic, SimpleConnection from) {
        super(consumerId);
        this.topic = topic;
        this.groupId = groupId;
        this.from = from;
    }
    
    public String getTopic() {
        return topic;
    }

    public String getGroupId() {
        return groupId;
    }

    @JsonIgnore
    public SimpleConnection getConnection () {
        return from;
    }
    
    public List<PartitionInfo> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<PartitionInfo> partitions) {
        this.partitions = partitions;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("id: ")
            .append(id)
            .append(" topic ")
            .append(topic)
            .append(" group: ")
            .append(groupId);
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((from == null) ? 0 : from.hashCode());
        result = prime * result + ((groupId == null) ? 0 : groupId.hashCode());
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ConsumerDesc other = (ConsumerDesc) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (from == null) {
            if (other.from != null)
                return false;
        } else if (!from.equals(other.from))
            return false;
        if (groupId == null) {
            if (other.groupId != null)
                return false;
        } else if (!groupId.equals(other.groupId))
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }
}

package com.dp.blackhole.common;

public class TopicPartitionKey {
    private String topic;
    private String partitionName;
    
    public TopicPartitionKey(String topic, String partitionName) {
        this.topic = topic;
        this.partitionName = partitionName;
    }
    
    public String getTopic() {
        return this.topic;
    }

    public String getPartition() {
        return this.partitionName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((partitionName == null) ? 0 : partitionName.hashCode());
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
        TopicPartitionKey other = (TopicPartitionKey) obj;
        if (partitionName == null) {
            if (other.partitionName != null)
                return false;
        } else if (!partitionName.equals(other.partitionName))
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }
}

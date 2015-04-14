package com.dp.blackhole.consumer;

public class PartitionTopicKey {
    private String topic;
    private String partitionName;
    
    public PartitionTopicKey(String topic, String partitionName) {
        this.topic = topic;
        this.partitionName = partitionName;
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
        PartitionTopicKey other = (PartitionTopicKey) obj;
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

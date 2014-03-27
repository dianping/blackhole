package com.dp.blackhole.supervisor;

public class ConsumerGroup {
    private String id;
    private String topic;
    
    public ConsumerGroup(String groupId, String topic) {
        this.id = groupId;
        this.topic = topic;
    }

    public String getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
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
        ConsumerGroup other = (ConsumerGroup) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return "ConsumerGroup[" + id + "/" + topic + "]";
    }
    
}
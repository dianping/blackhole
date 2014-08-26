package com.dp.blackhole.supervisor;

public class ConsumerGroup {
    private String goupId;
    private String topic;
    
    public ConsumerGroup(String groupId, String topic) {
        this.goupId = groupId;
        this.topic = topic;
    }

    public String getGroupId() {
        return goupId;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((goupId == null) ? 0 : goupId.hashCode());
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
        if (goupId == null) {
            if (other.goupId != null)
                return false;
        } else if (!goupId.equals(other.goupId))
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
        return "ConsumerGroup[" + topic + "/" + goupId + "]";
    }
    
}
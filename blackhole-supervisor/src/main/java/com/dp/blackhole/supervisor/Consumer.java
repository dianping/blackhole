package com.dp.blackhole.supervisor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.network.SimpleConnection;

public class Consumer {
    public static final Log LOG = LogFactory.getLog(Consumer.class);
    
    private String id;
    private String topic;
    private ConsumerGroup group;
    private SimpleConnection from;
    
    public Consumer(String id, ConsumerGroup group, String topic, SimpleConnection from) {
        this.id = id;
        this.group = group;
        this.topic = topic;
        this.from = from;
    }

    public String getId() {
        return id;
    }

    public ConsumerGroup getConsumerGroup() {
        return group;
    }
    
    public SimpleConnection getConnection () {
    	return from;
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((from == null) ? 0 : from.hashCode());
		result = prime * result + ((group == null) ? 0 : group.hashCode());
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
		Consumer other = (Consumer) obj;
		if (from == null) {
			if (other.from != null)
				return false;
		} else if (!from.equals(other.from))
			return false;
		if (group == null) {
			if (other.group != null)
				return false;
		} else if (!group.equals(other.group))
			return false;
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
    
}

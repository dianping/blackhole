package com.dp.blackhole.consumer;

import java.util.concurrent.atomic.AtomicLong;

public class PartitionTopicInfo {

    public final String topic;

    private String brokerString;

    private final AtomicLong consumedOffset;

    private final AtomicLong fetchedOffset;

    private final AtomicLong consumedOffsetChanged = new AtomicLong(0);

    final String partition;
    
    public PartitionTopicInfo(String topic,
            String partition,
            long consumedOffset,
            long fetchedOffset) {
        this.topic = topic;
        this.partition = partition;
        this.consumedOffset = new AtomicLong(consumedOffset);
        this.fetchedOffset = new AtomicLong(fetchedOffset);
    }

    public PartitionTopicInfo(String topic,
            String partition,
            String brokerString,
            long consumedOffset,
            long fetchedOffset) {
        this.topic = topic;
        this.partition = partition;
        this.brokerString = brokerString;
        this.consumedOffset = new AtomicLong(consumedOffset);
        this.fetchedOffset = new AtomicLong(fetchedOffset);
    }

    public String getBrokerString() {
        return brokerString;
    }

    public void setBrokerString(String brokerString) {
        this.brokerString = brokerString;
    }

    public long getConsumedOffset() {
        return consumedOffset.get();
    }

    public AtomicLong getConsumedOffsetChanged() {
        return consumedOffsetChanged;
    }

    public boolean updateComsumedOffsetChanged(long lastChanged) {
        return consumedOffsetChanged.compareAndSet(lastChanged, 0);
    }

    public long getFetchedOffset() {
        return fetchedOffset.get();
    }

    public void resetConsumeOffset(long newConsumeOffset) {
        consumedOffset.set(newConsumeOffset);
        consumedOffsetChanged.incrementAndGet();
    }

    public void updateFetchOffset(long newFetchOffset) {
        fetchedOffset.set(newFetchOffset);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((partition == null) ? 0 : partition.hashCode());
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
        PartitionTopicInfo other = (PartitionTopicInfo) obj;
        if (partition == null) {
            if (other.partition != null)
                return false;
        } else if (!partition.equals(other.partition))
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
        return topic + "-" + partition + "-" + brokerString + ", fetched/consumed offset: " + fetchedOffset.get() + "/" + consumedOffset.get();
    }
}

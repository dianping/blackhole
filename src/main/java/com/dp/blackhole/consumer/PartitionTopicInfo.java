package com.dp.blackhole.consumer;

import java.util.concurrent.atomic.AtomicLong;

public class PartitionTopicInfo {

    public final String topic;

    public final String brokerString;

    private final AtomicLong consumedOffset;

    private final AtomicLong fetchedOffset;

    private final AtomicLong consumedOffsetChanged = new AtomicLong(0);

    final String partition;

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

    public long getConsumedOffset() {
        return consumedOffset.get();
    }

    public AtomicLong getConsumedOffsetChanged() {
        return consumedOffsetChanged;
    }

    public boolean resetComsumedOffsetChanged(long lastChanged) {
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
    public String toString() {
        return topic + "-" + partition + "-" + brokerString + ", fetched/consumed offset: " + fetchedOffset.get() + "/" + consumedOffset.get();
    }
}

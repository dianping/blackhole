package com.dp.blackhole.consumer.api;

public class TailOffsetStrategy implements OffsetStrategy {

    @Override
    public long getOffset(String topic, String partitionId, long endOffset, long committedOffset) {
        return endOffset;
    }
}

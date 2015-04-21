package com.dp.blackhole.consumer.api;


public class CommittedOffsetStrategy implements OffsetStrategy {

    @Override
    public long getOffset(String topic, String partitionId, long endOffset, long committedOffset) {
        return committedOffset;
    }

}

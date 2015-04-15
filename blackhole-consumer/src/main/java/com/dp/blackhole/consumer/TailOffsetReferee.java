package com.dp.blackhole.consumer;

public class TailOffsetReferee implements OffsetReferee {

    @Override
    public long getOffsetByPartition(String topic, String partitionId) {
        return OffsetReferee.TAIL_OFFSET;
    }
}

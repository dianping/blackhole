package com.dp.blackhole.consumer;

public class TailOffsetReferee implements OffsetReferee {
    
    @Override
    public void updateOffset(String topic, String partitionName, long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getOffsetByPartition(String topic, String partitionId) {
        return USELESS_OFFSET;
    }

    @Override
    public boolean isActive() {
        return true;
    }
}

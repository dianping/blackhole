package com.dp.blackhole.consumer;

import com.dp.blackhole.storage.MessageAndOffset;

public interface OffsetReferee {
    public static final long LAST_COMMITTED_OFFSET = MessageAndOffset.UNINITIALIZED_OFFSET;
    public static final long TAIL_OFFSET = MessageAndOffset.OFFSET_OUT_OF_RANGE;
    
    /**
     * return the consumed offset user-defined.
     * @param topic
     * @param partitionId
     * @return
     */
    public long getOffsetByPartition(String topic, String partitionId);
}

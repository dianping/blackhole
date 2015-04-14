package com.dp.blackhole.consumer;

import com.dp.blackhole.storage.MessageAndOffset;

public interface OffsetReferee {
    public static final long USELESS_OFFSET = MessageAndOffset.UNINITIALIZED_OFFSET;
    
    /**
     * update the consumed offset user-defined.
     * This method is not necessary, so the implement can empty it.
     * @param topic
     * @param partitionId
     * @param offset
     */
    public void updateOffset(String topic, String partitionId, long offset);
    
    /**
     * return the consumed offset user-defined.
     * @param topic
     * @param partitionId
     * @return
     */
    public long getOffsetByPartition(String topic, String partitionId);
    
    /**
     * return true if offset referee works.
     * Consumer will invoke this function in any time,
     * so the implement must return the newest value.
     * @return
     */
    public boolean isActive();
}

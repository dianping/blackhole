package com.dp.blackhole.consumer;

import com.dp.blackhole.storage.MessageAndOffset;

public class ConsumerMessage {
    public static final long OFFSET_OUT_OF_RANGE = -3L;
    public static final long UNINITIALIZED_OFFSET= -1L;
    
    private final String content;
    private final long offset;
    private final String partition;

    public ConsumerMessage(MessageAndOffset messageAndOffset, String partition) {
        this.content = messageAndOffset.getMessageContent();
        this.offset = messageAndOffset.getOffset();
        this.partition = partition;
    }

    public String getContent() {
        return content;
    }

    public long getOffset() {
        return offset;
    }

    public String getPartition() {
        return partition;
    }
    
}

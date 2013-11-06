package com.dp.blackhole.collectornode.persistent;

public class MessageAndOffset {
    public final Message message;
    public final long offset;
    
    public MessageAndOffset(Message message, long offset) {
        this.message = message;
        this.offset = offset;
    }
}

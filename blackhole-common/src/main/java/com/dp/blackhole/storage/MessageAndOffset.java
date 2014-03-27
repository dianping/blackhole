package com.dp.blackhole.storage;

public class MessageAndOffset {
    
    public static final long OFFSET_OUT_OF_RANGE = -3L;
    
    public final Message message;
    public final long offset;
    
    public MessageAndOffset(Message message, long offset) {
        this.message = message;
        this.offset = offset;
    }
}

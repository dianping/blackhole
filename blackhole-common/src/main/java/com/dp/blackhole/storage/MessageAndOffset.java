package com.dp.blackhole.storage;

public class MessageAndOffset {
    
    public static final long OFFSET_OUT_OF_RANGE = -3L;
    public static final long UNINITIALIZED_OFFSET= -1L;
    
    private final Message message;
    private final long offset;
    
    public MessageAndOffset(Message message, long offset) {
        this.message = message;
        this.offset = offset;
    }

    public Message getMessage() {
        return message;
    }

    public long getOffset() {
        return offset;
    }
    
    public String getMessageContent() {
        return message.getContent();
    }
}

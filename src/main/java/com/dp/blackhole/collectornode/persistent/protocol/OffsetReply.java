package com.dp.blackhole.collectornode.persistent.protocol;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class OffsetReply extends NonDelegationTypedWrappable {
    
    private ByteBuffer head;
    private long offset;
    private String topic;
    private String partition;
    private int size;
    
    public OffsetReply() {}
    
    public OffsetReply(String topic, String partition, long offset) {
        this.head = allocateHead();
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.size = getSize();
        this.head.putInt(size);
        this.head.flip();
    }
    
    private ByteBuffer allocateHead() {
        return ByteBuffer.allocate((Integer.SIZE)/8);
    }
    
    @Override
    public int getType() {
        return DataMessageTypeFactory.OffsetReply;
    }

    @Override
    public void read(ByteBuffer buffer) {
        offset = buffer.getLong();
    }

    @Override
    public void write(ByteBuffer buffer) {
        buffer.putLong(offset);
        buffer.rewind();
    }

    @Override
    public int getSize() {
        return GenUtil.getStringSize(topic) + GenUtil.getStringSize(partition) + Long.SIZE/8;
    }

    public long getOffset() {
        return offset;
    }

    public String getTopic() {
        return topic;
    }

    public String getPartition() {
        return partition;
    }
}

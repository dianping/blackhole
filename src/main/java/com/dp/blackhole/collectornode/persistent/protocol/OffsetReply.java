package com.dp.blackhole.collectornode.persistent.protocol;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class OffsetReply extends NonDelegationTypedWrappable {
    
    private final ByteBuffer head = ByteBuffer.allocate(Integer.SIZE/8);
    private long offset;
    
    public OffsetReply() {}
    
    public OffsetReply(long offset) {
        this.offset = offset;
        head.putInt(Long.SIZE/8);
        head.flip();
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
        return Long.SIZE/8;
    }

    public long getOffset() {
        return offset;
    }
}

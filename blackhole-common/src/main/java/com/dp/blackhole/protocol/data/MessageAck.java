package com.dp.blackhole.protocol.data;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class MessageAck extends NonDelegationTypedWrappable {
    private short flag;
    private long offset;

    public MessageAck() {
    }

    public MessageAck(Boolean success, long offset) {
        if (success) {
            this.flag = 1;
        } else {
            this.flag = 0;
        }
        this.offset = offset;
    }

    public Boolean getResult() {
        if (flag == 1) {
            return true;
        } else {
            return false;
        }
    }
    
    public long getOffset() {
        return this.offset;
    }

    @Override
    public int getSize() {
        return (Short.SIZE + Long.SIZE) / 8;
    }

    @Override
    public void read(ByteBuffer buffer) {
        flag = buffer.getShort();
        offset = buffer.getLong();
    }

    @Override
    public void write(ByteBuffer buffer) {
        buffer.putShort(flag);
        buffer.putLong(offset);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.MessageAck;
    }
}

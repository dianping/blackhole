package com.dp.blackhole.collectornode.persistent.protocol;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class FetchRequest extends NonDelegationTypedWrappable {
    public String topic;
    public String partitionId;
    public long offset;
    public int limit;
    
    public FetchRequest() {
    }
    
    public FetchRequest(String topic, String partitionId, long offset, int limit) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    public int getSize() {
        return GenUtil.getStringSize(topic) + GenUtil.getStringSize(partitionId) + Long.SIZE/8 + Integer.SIZE/8;
    }

    @Override
    public void read(ByteBuffer buffer) {
        topic = GenUtil.readString(buffer);
        partitionId = GenUtil.readString(buffer);
        offset = buffer.getLong();
        limit = buffer.getInt();
    }

    @Override
    public void write(ByteBuffer buffer) {
        GenUtil.writeString(topic, buffer);
        GenUtil.writeString(partitionId, buffer);
        buffer.putLong(offset);
        buffer.putInt(limit);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.FetchRequest;
    }
}

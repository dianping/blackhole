package com.dp.blackhole.protocol.data;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class MicroBatchRequest extends NonDelegationTypedWrappable {
    public String topic;
    public String partitionId;
    public long rollPeriod;
    public long offset;
    
    public MicroBatchRequest() {
    }
    
    public MicroBatchRequest(String topic, String partitionId, long rollPeriod, long offset) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.rollPeriod = rollPeriod;
        this.offset = offset;
    }
    
    @Override
    public int getSize() {
        return GenUtil.getStringSize(topic) + GenUtil.getStringSize(partitionId) + Long.SIZE/8 + Long.SIZE/8;
    }

    @Override
    public void read(ByteBuffer buffer) {
        topic = GenUtil.readString(buffer);
        partitionId = GenUtil.readString(buffer);
        rollPeriod = buffer.getLong();
        offset = buffer.getLong();
    }

    @Override
    public void write(ByteBuffer buffer) {
        GenUtil.writeString(topic, buffer);
        GenUtil.writeString(partitionId, buffer);
        buffer.putLong(rollPeriod);
        buffer.putLong(offset);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.MicroBatchRequest;
    }

}

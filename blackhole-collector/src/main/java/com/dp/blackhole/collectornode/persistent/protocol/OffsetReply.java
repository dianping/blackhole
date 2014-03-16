package com.dp.blackhole.collectornode.persistent.protocol;

import java.nio.ByteBuffer;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class OffsetReply extends NonDelegationTypedWrappable {
    
    private long offset;
    private String topic;
    private String partition;
    
    public OffsetReply() {}
    
    public OffsetReply(String topic, String partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }
    
    @Override
    public int getType() {
        return DataMessageTypeFactory.OffsetReply;
    }

    @Override
    public void read(ByteBuffer buffer) {
        offset = buffer.getLong();
        topic = Util.readString(buffer);
        partition = Util.readString(buffer);
    }

    @Override
    public void write(ByteBuffer buffer) {
        buffer.putLong(offset);
        Util.writeString(topic, buffer);
        Util.writeString(partition, buffer);
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

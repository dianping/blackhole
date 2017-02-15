package com.dp.blackhole.protocol.data;

import java.nio.ByteBuffer;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.MessageSet;

public class ProduceRequest extends NonDelegationTypedWrappable {
    public String topic;
    public String partitionId;
    public long offset;
    ByteBufferMessageSet messages;
    
    public ProduceRequest() {}

    public ProduceRequest(String topic, String partitionId, ByteBufferMessageSet messages, long offset) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.messages = messages;
        this.offset = offset;
    }

    public MessageSet getMesssageSet() {
        return messages;
    }

    public int getMessageSize() {
        return (int) messages.getValidSize();
    }

    @Override
    public int getSize() {
        return Long.SIZE / 8 + GenUtil.getStringSize(topic) + GenUtil.getStringSize(partitionId) + (int) messages.getValidSize();
    }

    @Override
    public void read(ByteBuffer buffer) {
        topic = GenUtil.readString(buffer);
        try {
            partitionId = GenUtil.readString(buffer);
        } catch (RuntimeException e) {
            //TODO reminder we to remove the LOG when the bug is fixed
            Util.LOG.fatal(topic, e);
            throw e;
        }
        offset = buffer.getLong();
        messages = new ByteBufferMessageSet(buffer.slice());
    }

    @Override
    public void write(ByteBuffer buffer) {
        GenUtil.writeString(topic, buffer);
        GenUtil.writeString(partitionId, buffer);
        buffer.putLong(offset);
        messages.write(buffer);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.ProduceRequest;
    }
    
}

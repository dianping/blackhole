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
    ByteBufferMessageSet messages;
    
    public ProduceRequest() {}

    public ProduceRequest(String topic, String partitionId, ByteBufferMessageSet messages) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.messages = messages;
    }

    public MessageSet getMesssageSet() {
        return messages;
    }

    @Override
    public int getSize() {
        return GenUtil.getStringSize(topic) + GenUtil.getStringSize(partitionId) + messages.getSize();
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
        messages = new ByteBufferMessageSet(buffer.slice());
    }

    @Override
    public void write(ByteBuffer buffer) {
        GenUtil.writeString(topic, buffer);
        GenUtil.writeString(partitionId, buffer);
        messages.write(buffer);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.ProduceRequest;
    }
    
}

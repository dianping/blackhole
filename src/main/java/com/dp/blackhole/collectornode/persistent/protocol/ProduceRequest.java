package com.dp.blackhole.collectornode.persistent.protocol;

import java.nio.ByteBuffer;

import com.dp.blackhole.collectornode.persistent.ByteBufferMessageSet;
import com.dp.blackhole.collectornode.persistent.MessageSet;
import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class ProduceRequest extends NonDelegationTypedWrappable {
    public String topic;
    public String partitionId;
    ByteBufferMessageSet messages;
    
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
        partitionId = GenUtil.readString(buffer);
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
        return DataMessageTypeFactory.produceRequest;
    }
    
}

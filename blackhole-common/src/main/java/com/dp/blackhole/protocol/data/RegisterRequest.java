package com.dp.blackhole.protocol.data;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class RegisterRequest extends NonDelegationTypedWrappable {
    public String topic;
    public String sourceIdentify;
    public long peroid;
    public String broker;
    
    public RegisterRequest() {
    }
    
    public RegisterRequest(String topic, String sourceIdentify, long peroid, String broker) {
        this.topic = topic;
        this.sourceIdentify = sourceIdentify;
        this.peroid = peroid;
        this.broker = broker;
    }
    
    @Override
    public int getSize() {
        return GenUtil.getStringSize(topic) + GenUtil.getStringSize(sourceIdentify) + Long.SIZE/8 + GenUtil.getStringSize(broker);
    }

    @Override
    public void read(ByteBuffer buffer) {
        topic = GenUtil.readString(buffer);
        sourceIdentify = GenUtil.readString(buffer);
        peroid = buffer.getLong();
        broker = GenUtil.readString(buffer);
    }

    @Override
    public void write(ByteBuffer buffer) {
        GenUtil.writeString(topic, buffer);
        GenUtil.writeString(sourceIdentify, buffer);
        buffer.putLong(peroid);
        GenUtil.writeString(broker, buffer);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.RegisterRequest;
    }
}

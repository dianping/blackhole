package com.dp.blackhole.collectornode.persistent.protocol;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class RegisterRequest extends NonDelegationTypedWrappable {
    public String topic;
    public String source;
    public long peroid;
    public String broker;
    
    public RegisterRequest() {
    }
    
    public RegisterRequest(String topic, String source, long peroid, String broker) {
        this.topic = topic;
        this.source = source;
        this.peroid = peroid;
        this.broker = broker;
    }
    
    @Override
    public int getSize() {
        return GenUtil.getStringSize(topic) + GenUtil.getStringSize(source) + Long.SIZE/8 + GenUtil.getStringSize(broker);
    }

    @Override
    public void read(ByteBuffer buffer) {
        topic = GenUtil.readString(buffer);
        source = GenUtil.readString(buffer);
        peroid = buffer.getLong();
        broker = GenUtil.readString(buffer);
    }

    @Override
    public void write(ByteBuffer buffer) {
        GenUtil.writeString(topic, buffer);
        GenUtil.writeString(source, buffer);
        buffer.putLong(peroid);
        GenUtil.writeString(broker, buffer);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.RegisterRequest;
    }
}

package com.dp.blackhole.protocol.data;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class HeartBeatRequest extends NonDelegationTypedWrappable {
    public short heartbeat;
    
    public HeartBeatRequest() {
    }
    
    public HeartBeatRequest(short heartbeat) {
        this.heartbeat = heartbeat;
    }
    
    @Override
    public int getSize() {
        return Short.SIZE/8;
    }

    @Override
    public void read(ByteBuffer buffer) {
        heartbeat = buffer.getShort();
    }

    @Override
    public void write(ByteBuffer buffer) {
        buffer.putShort(heartbeat);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.Heartbeat;
    }

}

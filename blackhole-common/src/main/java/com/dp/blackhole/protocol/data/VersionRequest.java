package com.dp.blackhole.protocol.data;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class VersionRequest extends NonDelegationTypedWrappable {
    public short version;
    
    public VersionRequest() {
    }
    
    public VersionRequest(short version) {
        this.version = version;
    }
    
    @Override
    public int getSize() {
        return Short.SIZE/8;
    }

    @Override
    public void read(ByteBuffer buffer) {
        version = buffer.getShort();
    }

    @Override
    public void write(ByteBuffer buffer) {
        buffer.putShort(version);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.VersionRequest;
    }

}

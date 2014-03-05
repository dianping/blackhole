package com.dp.blackhole.common;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class PBmsg extends NonDelegationTypedWrappable {
    public ByteBuffer buf;
    
    public PBmsg(ByteBuffer buffer) {
        buf = buffer;
    }
    
    public PBmsg() {
    }
    
    @Override
    public int getSize() {
        return buf.capacity();
    }

    @Override
    public void read(ByteBuffer contentBuffer) {
        buf = contentBuffer;
    }

    @Override
    public void write(ByteBuffer contentBuffer) {
        contentBuffer.put(buf);
    }

    @Override
    public int getType() {
        return 0;
    }

}

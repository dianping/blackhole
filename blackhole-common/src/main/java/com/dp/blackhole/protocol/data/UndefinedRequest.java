package com.dp.blackhole.protocol.data;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.NonDelegationTypedWrappable;

class UndefinedRequest extends NonDelegationTypedWrappable {
    
    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public void read(ByteBuffer buffer) {
    }

    @Override
    public void write(ByteBuffer buffer) {
        throw new UnsupportedOperationException("not support in UndefinedRequest class");
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.UndefinedRequest;
    }

}

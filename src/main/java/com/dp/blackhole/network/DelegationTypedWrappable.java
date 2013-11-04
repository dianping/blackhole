package com.dp.blackhole.network;

import java.nio.ByteBuffer;

public abstract class DelegationTypedWrappable implements TypedWrappable {

    @Override
    public final boolean delegatable() {
        return true;
    }

    @Override
    public final int getSize() {
        return 0;
    }
    
    @Override
    public final void read(ByteBuffer contentBuffer) {
        throw new UnsupportedOperationException("not support in DelegationTypedWrappabe classes");
    }

    @Override
    public final void write(ByteBuffer contentBuffer) {
        throw new UnsupportedOperationException("not support in DelegationTypedWrappabe classes");
    }
    
}

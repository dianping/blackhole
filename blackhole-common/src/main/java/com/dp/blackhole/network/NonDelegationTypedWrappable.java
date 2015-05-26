package com.dp.blackhole.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

public abstract class NonDelegationTypedWrappable implements TypedWrappable {

    @Override
    public final boolean delegatable() {
        return false;
    }

    @Override
    public final boolean complete() {
        throw new UnsupportedOperationException("not support in NonDelegationTypedWrappabe classes");
    }
    
    @Override
    public final int read(ScatteringByteChannel channel) throws IOException {
        throw new UnsupportedOperationException("not support in NonDelegationTypedWrappabe classes");
    }

    @Override
    public final int write(GatheringByteChannel channel) throws IOException {
        throw new UnsupportedOperationException("not support in NonDelegationTypedWrappabe classes");
    }

}

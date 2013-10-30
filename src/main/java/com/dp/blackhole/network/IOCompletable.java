package com.dp.blackhole.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

public interface IOCompletable {
    
    public boolean complete();

    public int read(ScatteringByteChannel channel) throws IOException;

    public int write(GatheringByteChannel channel) throws IOException;
}

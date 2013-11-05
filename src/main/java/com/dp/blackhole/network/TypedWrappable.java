package com.dp.blackhole.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

public interface TypedWrappable extends Typed, IOCompletable {

    public int getSize();

    public boolean delegatable();

    public void read(ByteBuffer contentBuffer);
    
    public void write(ByteBuffer contentBuffer);

    public int read(ScatteringByteChannel channel) throws IOException;
    
    public int write(GatheringByteChannel channel) throws IOException; 
}

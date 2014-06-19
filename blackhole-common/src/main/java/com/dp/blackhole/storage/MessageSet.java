package com.dp.blackhole.storage;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

public interface MessageSet {

    public int write(GatheringByteChannel channel, long offset, int length) throws IOException; 

    public int getSize();
}

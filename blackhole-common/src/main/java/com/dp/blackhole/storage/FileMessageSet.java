package com.dp.blackhole.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;

public class FileMessageSet implements MessageSet{
    FileChannel channel;
    // it is the real offset, the offset in the file
    long offset;
    int length;
    
    public FileMessageSet(FileChannel _channel) {
        channel = _channel;
    }
    
    public FileMessageSet(FileChannel channel, long offset, int length) {
        this.channel = channel;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int write(GatheringByteChannel target, long _offset, int length)
            throws IOException {
        long written =  channel.transferTo(offset + _offset, length, target);
        if (written > Integer.MAX_VALUE) {
            throw new RuntimeException("MessageSet.write is limited to Integer.Max");
        }
        return (int)written;
    }

    @Override
    public int getSize() {
        return length;
    } 
    
    public long getOffset () {
        return offset;
    }
}

package com.dp.blackhole.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class ByteBufferChannel implements GatheringByteChannel{
    private ByteBuffer byteBuffer;
    
    public ByteBufferChannel (ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
}
    
    @Override
    public int write(ByteBuffer src) throws IOException {
        int ori = byteBuffer.position();
        byteBuffer.put(src);
        return byteBuffer.position() - ori;
    }

    @Override
    public boolean isOpen() {
        return byteBuffer != null;
    }

    @Override
    public void close() throws IOException {
        
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException {
        throw new IOException("not supproted");
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        throw new IOException("not supproted");
    }

}

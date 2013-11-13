package com.dp.blackhole.collectornode.persistent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ByteBufferMessageSet implements MessageSet{
    ByteBuffer buffer;
    long startOffset;
    long validSize;
    
    public ByteBufferMessageSet(ByteBuffer buffer, long startOffset) {
        this.buffer = buffer;
        this.startOffset = startOffset;
        this.validSize = calcValidSize();
    }
    
    public ByteBufferMessageSet(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public void write(ByteBuffer buffer) {
        buffer.put(this.buffer);
    }
    
    public Iterator<MessageAndOffset> getItertor() {
        return new Iter();
    }
    
    public class Iter implements Iterator<MessageAndOffset> {
        private boolean hasNext;
        private int nextLength;
        private long currentOffset;
        private ByteBuffer viewBuf;
        
        public Iter() {
            currentOffset = startOffset;
            viewBuf = buffer.duplicate();
            if (viewBuf.remaining() < 4) {
                hasNext = false;
            } else {
                hasNext = true;
            }
        }
        
        @Override
        public boolean hasNext() {
            if (hasNext) {
                if (viewBuf.remaining() < 4) {
                    return false;
                }
                nextLength = viewBuf.getInt();
                if (viewBuf.remaining() < nextLength) {
                    hasNext = false;
                }
            }
            return hasNext;
        }

        @Override
        public MessageAndOffset next() {
            if (!hasNext) {
                throw new NoSuchElementException();
            }
            viewBuf.limit(viewBuf.position() + nextLength);
            ByteBuffer buf = viewBuf.slice();
            viewBuf.limit(viewBuf.capacity());
            currentOffset += nextLength;
            return new MessageAndOffset(new Message(buf), currentOffset);
        }
        
        @Override
        public void remove() {
            // TODO Auto-generated method stub
            
        }
        
    }

    @Override
    public long write(GatheringByteChannel channel, int offset, int length)
            throws IOException {
        int written = 0;
        while (buffer.hasRemaining()) {
            written += channel.write(buffer);
        }
        return written;
    }

    @Override
    public int getSize() {
        // TODO Auto-generated method stub
        return buffer.capacity();
    }
    
    private long calcValidSize() {
        Iterator<MessageAndOffset> iter = getItertor();
        MessageAndOffset last = null;
        while (iter.hasNext()) {
            last = iter.next();
        }
        return last.offset - startOffset;
    }
    
    public long getValidSize() {
        return validSize;
    }
}

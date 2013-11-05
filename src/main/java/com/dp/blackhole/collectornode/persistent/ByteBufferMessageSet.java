package com.dp.blackhole.collectornode.persistent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ByteBufferMessageSet implements MessageSet{
    ByteBuffer buffer;
    
    public ByteBufferMessageSet(ByteBuffer buffer) {
        this.buffer = buffer;
    }
    
    public void write(ByteBuffer buffer) {
        buffer.put(this.buffer);
    }
    
    public Iterator<Message> getItertor() {
        return new Iter();
    }
    
    public class Iter implements Iterator<Message> {
        private boolean hasNext;
        private int nextLength;
        
        public Iter() {
            if (buffer.remaining() < 4) {
                hasNext = false;
            } else {
                hasNext = true;
            }
        }
        
        @Override
        public boolean hasNext() {
            if (hasNext) {
                if (buffer.remaining() < 4) {
                    return false;
                }
                nextLength = buffer.getInt();
                if (buffer.remaining() < nextLength) {
                    hasNext = false;
                }
            }
            return hasNext;
        }

        @Override
        public Message next() {
            if (!hasNext) {
                throw new NoSuchElementException();
            }
            int originLimit = buffer.limit();
            buffer.limit(buffer.position() + nextLength);
            ByteBuffer buf = buffer.slice();
            buffer.limit(originLimit);
            return new Message(buf);
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
}

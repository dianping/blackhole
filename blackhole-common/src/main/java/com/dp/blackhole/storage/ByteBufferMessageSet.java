package com.dp.blackhole.storage;

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
        this.validSize = calcValidSize();
    }

    public void write(ByteBuffer buffer) {
        buffer.put(this.buffer);
    }
    
    public Iterator<MessageAndOffset> getItertor() {
        return new Iter();
    }
    
    public class Iter implements Iterator<MessageAndOffset> {
        private boolean hasNext;
        private boolean ready;
        MessageAndOffset current;
        private long currentOffset;
        private ByteBuffer viewBuf;
        
        public Iter() {
            currentOffset = startOffset;
            viewBuf = buffer.duplicate();
            ready = false;
        }
        
        private void compute() {
            if (ready) {
                return;
            }
            ready = true;
            
            if (viewBuf.remaining() < 4) {
                hasNext = false;
                return;
            }
            
            int nextLength = viewBuf.getInt();
            if (viewBuf.remaining() < nextLength) {
                hasNext = false;
                return;
            }
            
            int oriLimit = viewBuf.limit();
            viewBuf.limit(viewBuf.position() + nextLength);
            ByteBuffer buf = viewBuf.slice();
            viewBuf.position(viewBuf.limit());
            viewBuf.limit(oriLimit);
            
            current = new MessageAndOffset(new Message(buf), currentOffset);
            currentOffset += 4 + nextLength;
            hasNext = true;
        }
        
        @Override
        public boolean hasNext() {
            if (!ready) {
                compute();
            }
            return hasNext;
        }

        @Override
        public MessageAndOffset next() {
            if (!ready) {
                compute();
            }
            if (!hasNext) {
                throw new NoSuchElementException();
            } else {
                ready = false;
                return current;
            }
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }

    @Override
    public int write(GatheringByteChannel channel, long offset, int length)
            throws IOException {
        int written = 0;
        while (buffer.hasRemaining()) {
            written += channel.write(buffer);
        }
        return written;
    }

    @Override
    public int getSize() {
        return buffer.capacity();
    }
    
    private long calcValidSize() {
        Iterator<MessageAndOffset> iter = getItertor();
        MessageAndOffset last = null;
        while (iter.hasNext()) {
            last = iter.next();
        }
        if (last == null) {
            return 0;
        }
        // last offset + last message size (message length(4) + real message size)
        return last.getOffset() + last.getMessage().getSize() - startOffset;
    }
    
    public long getValidSize() {
        return validSize;
    }

    public void makeValid() {
        Iterator<MessageAndOffset> iter = getItertor();
        MessageAndOffset last = null;
        int messageSize = 0;
        while (iter.hasNext()) {
            last = iter.next();
            messageSize = messageSize + last.getMessage().getSize();
        }
        if (last == null) {
            buffer.limit(buffer.position());
            return;
        }
        buffer.limit(messageSize);
    }
}

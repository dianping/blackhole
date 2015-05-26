package com.dp.blackhole.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

public class TransferWrap implements Typed, IOCompletable {
    
    private ByteBuffer head;
    private ByteBuffer content;
    
    private TypedFactory wrappedFactory;
    
    private TypedWrappable wrapped;
    private int type;
    private int size;
    private boolean complete;
    
    public TransferWrap(TypedWrappable wrapped) {
        type = wrapped.getType();
        size = wrapped.getSize();
        head = allocateHead();
        head.putInt(type);
        head.putInt(size);
        head.flip();
        this.wrapped = wrapped;
        if (!this.wrapped.delegatable()) {
            content = ByteBuffer.allocate(wrapped.getSize());
            this.wrapped.write(content);
            content.flip();
        }
    }
    
    public TransferWrap(TypedFactory typedef) {
        this.head = allocateHead();
        this.wrappedFactory = typedef;
    }
    
    private ByteBuffer allocateHead() {
        return ByteBuffer.allocate(8);
    }
    
    @Override
    public int write(GatheringByteChannel channel) throws IOException {
        int written = 0;
        // write head
        if (head.hasRemaining()) {
            written += GenUtil.retryWrite(channel, head);
            if (head.hasRemaining()) {
                return written;
            }
        }
        
        // delegate write to wrapped object if delegatable
        // , else write content to channel 
        if (wrapped.delegatable()) {
            written += wrapped.write(channel);
            if (wrapped.complete()) {
                complete = true;
            }
        } else {
            if (content.hasRemaining()) {
                written += GenUtil.retryWrite(channel, content);
                if (!content.hasRemaining()) {
                    complete = true;
                    content = null;
                }
            }
        }
        return written;
    }
    
    @Override
    public int read(ScatteringByteChannel channel) throws IOException {
        int read = 0;
        // read head
        if (head.hasRemaining()) {
            int num = channel.read(head);
            if (num < 0) {
                throw new IOException("end-of-stream reached");
            }
            read += num;
            if (head.hasRemaining()) {
                return read;
            } else {
                head.flip();
                type = head.getInt();
                size = head.getInt();
                wrapped = wrappedFactory.getWrappedInstanceFromType(type);
                if (!wrapped.delegatable()) {
                    content = ByteBuffer.allocate(size);
                }
            }
        }
        
        // delegate read to wrapped object if delegatable
        // , else read content into channel
        if (wrapped.delegatable()) {
            read += wrapped.read(channel);
            if (wrapped.complete()) {
                complete = true;
            }
        } else {
            if (content.hasRemaining()) {
                int num = channel.read(content);
                if (num < 0) {
                    throw new IOException("end-of-stream reached");
                }
                read += num;
                if (!content.hasRemaining()) {
                    content.flip();
                    wrapped.read(content);
                    complete = true;
                    content = null;
                }
            }
        }
        return read;
    }

    public TypedWrappable unwrap() {
        return wrapped;
    }
    
    @Override
    public boolean complete() {
        return complete;
    }

    @Override
    public int getType() {
        return type;
    }
}

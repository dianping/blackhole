package com.dp.blackhole.broker.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.storage.FileMessageSet;
import com.dp.blackhole.storage.Message;
import com.dp.blackhole.storage.MessageSet;

public class Segment {

    public static final Log LOG = LogFactory.getLog(Segment.class);
    
    FileChannel channel;
    String file;
    private long startOffset;
    private AtomicLong endOffset;
    private long unflushSize;
    
    private long closeTimestamp;
    int splitThreshold;
    int flushThreshold;
    
    public Segment(String parent, long offset, boolean verify, boolean readonly, int splitThreshold, int flushThreshold) throws IOException {
        file = getFilePath(parent, offset);
        if (!readonly) {
            channel = new RandomAccessFile(getFilePath(parent, offset), "rw").getChannel();
        } else {
            channel = new RandomAccessFile(getFilePath(parent, offset), "r").getChannel();
        }
        startOffset = offset;
        if (!verify) {
            endOffset = new AtomicLong(startOffset + channel.size());
        } else {
            long effectiveLength = verifySegment();
            channel.position(effectiveLength);
            endOffset = new AtomicLong(startOffset + effectiveLength);
        }
        this.flushThreshold = flushThreshold;
        this.splitThreshold = splitThreshold;
        this.closeTimestamp = 0;
    }
    
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            LOG.error("Oops, failed to close channel", e);
        }
    }
    
    private long verifySegment() throws IOException {
        long effectiveLength = 0;
        long remaining = channel.size();
        ByteBuffer sizeBuf = ByteBuffer.allocate(Integer.SIZE/8);
        while (remaining > 0) {
            if (remaining < Integer.SIZE/8) {
                break;
            }
            int read = 0;
            while (sizeBuf.hasRemaining()) {
                int num = channel.read(sizeBuf);
                if (num == -1) {
                    break;
                }
                read += num;
            }
            if (sizeBuf.hasRemaining()) {
                break;
            }
            sizeBuf.flip();
            int messageSize = sizeBuf.getInt();
            sizeBuf.rewind();
            if (remaining-4 < messageSize) {
                break;
            }
            ByteBuffer messageBuf = ByteBuffer.allocate(messageSize);
            while (messageBuf.hasRemaining()) {
                int num = channel.read(messageBuf);
                if (num == -1) {
                    break;
                }
                read += num;
            }
            if (messageBuf.hasRemaining()) {
                break;
            }
            messageBuf.flip();
            Message message = new Message(messageBuf);
            if (!message.valid()) {
                break;
            }
            effectiveLength += read;
            remaining -= read;
        }
        if (effectiveLength < channel.size()) {
            channel.truncate(effectiveLength);
        }
        return effectiveLength;
    }
    
    public long getStartOffset() {
        return startOffset;
    }
    
    public long getEndOffset() {
        return endOffset.get();
    }

    private String getFilePath(String parent, long start_offset) {
        return parent + '/' + start_offset + ".blackhole";
    }

    long append(MessageSet messages) throws IOException {
        long written = messages.write(channel, 0 ,messages.getSize());
        unflushSize += written;
        endOffset.addAndGet(written);
        if (unflushSize > flushThreshold) {
            flush();
        }
        long length = endOffset.get() - startOffset;
        if (length > splitThreshold) {
            return startOffset + length;
        }
        return 0;
    }
    
    public void flush() throws IOException {
        if (unflushSize == 0) {
            return;
        }

        channel.force(true);
        unflushSize = 0;
    }

    public boolean contains(long offset) {
        if (offset >= startOffset && offset < getEndOffset()) {
            return true;
        } else {
            return false;
        }
    }

    public FileMessageSet read(long offset, int length) {
        long endOffset = getEndOffset();
        if (offset + length > endOffset) {
            length = (int) (endOffset - offset);
        }
        return new FileMessageSet(channel, offset - startOffset, length);
    }
    
    @Override
    public String toString() {
       return "segment[" + startOffset + "," + endOffset.get() + "]/" + closeTimestamp;
    }
    
    public Long getCloseTimestamp() {
        return closeTimestamp;
    }
    
    public void setCloseTimestamp(long ts) {
        closeTimestamp = ts;
    }
    
    public void destory() {
        try {
            channel.close();
        } catch (IOException e) {
            LOG.error("Oops, failed to close channel", e);
        }
        File f = new File(file);
        f.delete();
    }
}

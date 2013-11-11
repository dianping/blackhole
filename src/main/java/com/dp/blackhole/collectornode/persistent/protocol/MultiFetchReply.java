package com.dp.blackhole.collectornode.persistent.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.List;

import com.dp.blackhole.collectornode.persistent.MessageSet;
import com.dp.blackhole.network.DelegationTypedWrappable;
import com.dp.blackhole.network.GenUtil;

import static java.lang.String.format;

public class MultiFetchReply extends DelegationTypedWrappable {

    final ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.SIZE/8);
    private ByteBuffer contentBuffer = null;
    private List<String> partitionList;
    private List<MessageSet> messagesList;
    private List<Long> offsetList;
    private int size;
    private int index;
    private boolean complete;
    private FetchReply currentReply;
    
    public MultiFetchReply() {}
    
    public MultiFetchReply(List<String> partitionList, List<MessageSet> messagesList, List<Long> offsetList) {
        this.partitionList = partitionList;
        this.messagesList = messagesList;
        this.offsetList = offsetList;
        for (MessageSet set : messagesList) {
            size += set.getSize();
        }
        this.sizeBuffer.putInt(size);
        this.sizeBuffer.flip();
    }
    
    @Override
    public int getType() {
        return DataMessageTypeFactory.MultiFetchReply;
    }
    
    @Override
    public int getSize() {
        return size;
    }

    public List<String> getPartitionList() {
        return partitionList;
    }

    public List<MessageSet> getMessagesList() {
        return messagesList;
    }

    public List<Long> getOffsetList() {
        return offsetList;
    }

    @Override
    public int write(GatheringByteChannel channel) throws IOException {
        int written = 0;
        if (sizeBuffer.hasRemaining()) {
            written = GenUtil.retryWrite(channel, sizeBuffer);
            if (!sizeBuffer.hasRemaining()) {
                return written;
            }
        }
        if (!sizeBuffer.hasRemaining()) {
            if (currentReply == null) {
                if (messagesList.size() == 0) {
                    throw new IOException("MessageSet list empty!");
                }
                currentReply = new FetchReply(partitionList.get(0), messagesList.get(0), offsetList.get(0));
            }
            written += currentReply.write(channel);
            if (currentReply.complete()) {
                index++;
                if (index == messagesList.size()) {
                    complete = true;
                } else {
                    currentReply = new FetchReply(partitionList.get(index), messagesList.get(index), offsetList.get(index));
                }
            }
        }
        return written;
    }

    @Override
    public int read(ScatteringByteChannel channel) throws IOException {
        expectIncomplete();
        int read = 0;
        if (sizeBuffer.remaining() > 0) {
            read += channel.read(sizeBuffer);
        }
        //
        if (contentBuffer == null && !sizeBuffer.hasRemaining()) {
            sizeBuffer.rewind();
            int size = sizeBuffer.getInt();
            if (size <= 0) {
                throw new IOException(format("%d is not a valid request size.", size));
            }
            contentBuffer = byteBufferAllocate(size);
        }
        //
        if (contentBuffer != null) {
            read = channel.read(contentBuffer);
            //
            if (!contentBuffer.hasRemaining()) {
                contentBuffer.rewind();
                complete = true;
            }
        }
        return read;
    }

    @Override
    public boolean complete() {
        return complete;
    }
    
    public int readCompletely(ScatteringByteChannel channel) throws IOException {
        int read = 0;
        while (!complete()) {
            read += read(channel);
        }
        return read;
    }
    
    public void expectIncomplete() {
        if (complete()) {
            throw new IllegalStateException("This operation cannot be completed on a complete request.");
        }
    }
    
    public void expectComplete() {
        if (!complete()) {
            throw new IllegalStateException("This operation cannot be completed on an incomplete request.");
        }
    }
    
    public ByteBuffer buffer() {
        expectComplete();
        return contentBuffer;
    }
    
    private ByteBuffer byteBufferAllocate(int size) {
        ByteBuffer buffer = null;
        try {
            buffer = ByteBuffer.allocate(size);
        } catch (OutOfMemoryError oome) {
            throw new RuntimeException("OOME with size " + size, oome);
        } catch (RuntimeException t) {
            throw t;
        }
        return buffer;
    }
}

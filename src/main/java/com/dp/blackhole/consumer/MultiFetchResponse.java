package com.dp.blackhole.consumer;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.dp.blackhole.collectornode.persistent.ByteBufferMessageSet;
import com.dp.blackhole.collectornode.persistent.MessageAndOffset;

public class MultiFetchResponse implements Iterable<ByteBufferMessageSet> {

    private final List<ByteBufferMessageSet> messageSets;

    /**
     * create a multi-response
     * <p>
     * buffer format: <b> size+errorCode(short)+payload+size+errorCode(short)+payload+... </b>
     * <br/>
     * size = 2(short)+length(payload)
     * </p>
     * 
     * @param buffer the whole data buffer
     * @param numSets response count
     * @param offsets message offset for each response
     */
    public MultiFetchResponse(ByteBuffer buffer, int numSets, List<Long> offsets) {
        super();
        this.messageSets = new ArrayList<ByteBufferMessageSet>();
        for (int i = 0; i < numSets; i++) {
            //size of messages bytes excluding offset bytes
            int payloadSize = buffer.getInt();
            long offset = buffer.getLong();
            assertTrue(offset == offsets.get(i));//TODO
            ByteBuffer copy = buffer.slice();
            copy.limit(payloadSize);
            //move position for next reading
            buffer.position(buffer.position() + payloadSize);
            ByteBufferMessageSet messageSet = new ByteBufferMessageSet(copy, offsets.get(i));
            if (offset == MessageAndOffset.OFFSET_OUT_OF_RANGE) {
                messageSet.setOffsetOutOfRange(true);
            }
            messageSets.add(new ByteBufferMessageSet(copy, offsets.get(i)));
        }
    }

    public Iterator<ByteBufferMessageSet> iterator() {
        return messageSets.iterator();
    }

    public int size() {
        return messageSets.size();
    }
    
    public boolean isEmpty() {
        return size() == 0;
    }

}

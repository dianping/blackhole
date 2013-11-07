package com.dp.blackhole.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.dp.blackhole.collectornode.persistent.ByteBufferMessageSet;
import com.dp.blackhole.collectornode.persistent.protocol.FetchReply;
import com.dp.blackhole.collectornode.persistent.protocol.FetchRequest;
import com.dp.blackhole.collectornode.persistent.protocol.MultiFetchReply;
import com.dp.blackhole.collectornode.persistent.protocol.MultiFetchRequest;
import com.dp.blackhole.collectornode.persistent.protocol.OffsetReply;
import com.dp.blackhole.collectornode.persistent.protocol.OffsetRequest;
import com.dp.blackhole.network.TypedWrappable;

public class SimpleConsumer extends SimpleOperation {
    
    public SimpleConsumer(String host, int port) {
        super(host,port);
    }

    public SimpleConsumer(String host, int port, int soTimeout, int bufferSize) {
        super(host, port, soTimeout, bufferSize);
    }

    public ByteBufferMessageSet fetch(FetchRequest request) throws IOException {
        TypedWrappable response = send(request);
        return new ByteBufferMessageSet(((FetchReply)response).buffer(), request.offset);
    }

    public long getOffsetsBefore(String topic, String partition, long time, int maxNumOffsets) throws IOException {
        TypedWrappable response = send(new OffsetRequest(topic, partition, time, maxNumOffsets));
        return ((OffsetReply) response).getOffset();
    }

    public MultiFetchResponse multifetch(List<FetchRequest> fetches) throws IOException {
        TypedWrappable response = send(new MultiFetchRequest(fetches));
        List<Long> offsets = new ArrayList<Long>();
        for (FetchRequest fetch : fetches) {
            offsets.add(fetch.offset);
        }
        return new MultiFetchResponse(((MultiFetchReply)response).buffer(), fetches.size(), offsets);
    }

    public long getLatestOffset(String topic, String partition) throws IOException {
        return getOffsetsBefore(topic, partition, -1, 1);
    }
}

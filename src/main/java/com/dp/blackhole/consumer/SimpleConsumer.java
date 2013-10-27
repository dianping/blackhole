package com.dp.blackhole.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleConsumer extends SimpleOperation implements IConsumer {
    
    public SimpleConsumer(String host, int port) {
        super(host,port);
    }

    public SimpleConsumer(String host, int port, int soTimeout, int bufferSize) {
        super(host, port, soTimeout, bufferSize);
    }

    public ByteBufferMessageSet fetch(FetchRequest request) throws IOException {
        KV<Receive, ErrorMapping> response = send(request);
        return new ByteBufferMessageSet(response.k.buffer(), request.offset, response.v);
    }

    public long[] getOffsetsBefore(String topic, int partition, long time, int maxNumOffsets) throws IOException {
        KV<Receive, ErrorMapping> response = send(new OffsetRequest(topic, partition, time, maxNumOffsets));
        return OffsetRequest.deserializeOffsetArray(response.k.buffer());
    }

    public MultiFetchResponse multifetch(List<FetchRequest> fetches) throws IOException {
        KV<Receive, ErrorMapping> response = send(new MultiFetchRequest(fetches));
        List<Long> offsets = new ArrayList<Long>();
        for (FetchRequest fetch : fetches) {
            offsets.add(fetch.offset);
        }
        return new MultiFetchResponse(response.k.buffer(), fetches.size(), offsets);
    }

    @Override
    public long getLatestOffset(String topic, int partition) throws IOException {
        long[] result = getOffsetsBefore(topic, partition, -1, 1);
        return result.length == 0 ? -1 : result[0];
    }
}

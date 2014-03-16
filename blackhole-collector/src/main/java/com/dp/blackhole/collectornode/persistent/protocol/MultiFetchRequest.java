package com.dp.blackhole.collectornode.persistent.protocol;

import java.nio.ByteBuffer;
import java.util.List;

import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class MultiFetchRequest extends NonDelegationTypedWrappable {

    public List<FetchRequest> fetches;

    public MultiFetchRequest() {
    }

    public MultiFetchRequest(List<FetchRequest> fetches) {
        this.fetches = fetches;
    }

    @Override
    public int getSize() {
        int size = 2;
        for (FetchRequest fetch : fetches) {
            size += fetch.getSize();
        }
        return size;
    }

    @Override
    public void read(ByteBuffer buffer) {
        int count = buffer.getShort();
        for (int i = 0; i < count; i++) {
            fetches.get(i).read(buffer);
        }
    }

    @Override
    public void write(ByteBuffer buffer) {
        if (fetches.size() > Short.MAX_VALUE) {//max 32767
            throw new IllegalArgumentException("Number of requests in MultiFetchRequest exceeds " + Short.MAX_VALUE + ".");
        }
        buffer.putShort((short) fetches.size());
        for (FetchRequest fetch : fetches) {
            fetch.write(buffer);
        }
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.MultiFetchRequest;
    }
}

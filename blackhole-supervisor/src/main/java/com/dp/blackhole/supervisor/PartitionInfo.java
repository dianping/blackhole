package com.dp.blackhole.supervisor;

import java.util.concurrent.atomic.AtomicLong;

import com.dp.blackhole.network.SimpleConnection;

public class PartitionInfo {
    private String id;
    private SimpleConnection connection;
    private AtomicLong endOffset;
    
    public PartitionInfo(String id, SimpleConnection connection, long endOffset) {
        this.id = id;
        this.connection = connection;
        this.endOffset = new AtomicLong(endOffset);
    }
    
    public PartitionInfo(PartitionInfo info) {
        this.id = info.getId();
        this.connection = info.getConnection();
        this.endOffset = new AtomicLong(info.getEndOffset());
    }
    
    public void setEndOffset(long endOffset) {
        this.endOffset.set(endOffset);
    }

    public String getId() {
        return id;
    }

    public SimpleConnection getConnection() {
        return connection;
    }

    public long getEndOffset() {
        return endOffset.get();
    }
    
}

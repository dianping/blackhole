package com.dp.blackhole.supervisor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionInfo {
    private String id;
    private String host;
    private AtomicLong endOffset;
    private AtomicBoolean offline;
    
    public PartitionInfo(String id, String host, long endOffset) {
        this.id = id;
        this.host = host;
        this.endOffset = new AtomicLong(endOffset);
        this.offline = new AtomicBoolean(false);
    }
    
    public PartitionInfo(PartitionInfo info) {
        this.id = info.getId();
        this.host = info.getHost();
        this.endOffset = new AtomicLong(info.getEndOffset());
        this.offline = new AtomicBoolean(false);
    }
    
    public void setEndOffset(long endOffset) {
        this.endOffset.set(endOffset);
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public long getEndOffset() {
        return endOffset.get();
    }

    public void markOffline(boolean offline) {
        this.offline.getAndSet(offline);
    }
    
    public boolean isOffline () {
        return offline.get();
    }
    
}

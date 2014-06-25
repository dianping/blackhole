package com.dp.blackhole.broker.storage;


public class RollPartition {
    public Partition p;
    public long startOffset;
    public long length;
    
    public RollPartition(Partition partition) {
        p = partition;
    }

    public RollPartition(Partition partition, long start) {
        p = partition;
        startOffset = start;
    }
}

package com.dp.blackhole.collectornode.persistent;

public class RollPartition {
    public Partition p;
    public long startOffset;
    public int length;
    
    public RollPartition(Partition partition) {
        p = partition;
    }

    public RollPartition(Partition partition, long start) {
        p = partition;
        startOffset = start;
    }
}

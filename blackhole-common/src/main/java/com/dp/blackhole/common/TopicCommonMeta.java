package com.dp.blackhole.common;

import java.io.Serializable;

public class TopicCommonMeta implements Serializable {
    private static final long serialVersionUID = 1707380214769996094L;
    private int maxLineSize;
    private int minMsgSent;
    private int msgBufSize;
    private long rollPeriod;
    private int partitionFactor;
    
    public TopicCommonMeta(long rollPeriod, int maxLineSize,
            int minMsgSent, int msgBufSize, int partitionFactor) {
        this.rollPeriod = rollPeriod;
        this.maxLineSize = maxLineSize;
        this.minMsgSent = minMsgSent;
        this.msgBufSize = msgBufSize;
        this.partitionFactor = partitionFactor;
    }

    public long getRollPeriod() {
        return rollPeriod;
    }

    public void setRollPeriod(long rollPeriod) {
        this.rollPeriod = rollPeriod;
    }

    public int getMaxLineSize() {
        return maxLineSize;
    }

    public int getMinMsgSent() {
        return minMsgSent;
    }

    public void setMinMsgSent(int minMsgSent) {
        this.minMsgSent = minMsgSent;
    }

    public int getMsgBufSize() {
        return msgBufSize;
    }

    public void setMsgBufSize(int msgBufSize) {
        this.msgBufSize = msgBufSize;
    }

    public int getPartitionFactor() {
        return partitionFactor;
    }

    public void setPartitionFactor(int partitionFactor) {
        this.partitionFactor = partitionFactor;
    }
}

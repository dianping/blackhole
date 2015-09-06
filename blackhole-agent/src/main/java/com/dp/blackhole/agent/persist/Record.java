package com.dp.blackhole.agent.persist;

import java.io.Serializable;

import com.dp.blackhole.common.Util;

public class Record implements Serializable{
    private static final long serialVersionUID = 6803400542870701191L;
    
    public static final int ROLL = 1;
    public static final int ROTATE  = 2;
    
    private final int type;
    private final long rollTs;
    private final long startOffset;
    private final long endOffset;
    private final long rotation;
    private final long recordTs;
    
    public Record(int type, long rollTs, long startOffset, long endOffset, long rotation) {
        this.type = type;
        this.rollTs = rollTs;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.rotation = rotation;
        this.recordTs = System.currentTimeMillis();
    }
    
    public int getType() {
        return type;
    }
    
    public long getRollTs() {
        return rollTs;
    }

    public long getRecordTs() {
        return recordTs;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public long getRotation() {
        return rotation;
    }

    public String getTypeName() {
        String name;
        switch (this.type) {
        case ROLL:
            name = "ROLL";
            break;
        case ROTATE:
            name = "ROTATE";
            break;
        default:
            name = "UNKNOW";
            break;
        }
        return name;
    }

    @Override
    public String toString() {
        return "[" + getTypeName() + "\t" + rollTs + "\t"
                + Util.getTimeString(rollTs) + "\t"
                + String.format("% 10d", startOffset) + "\t"
                + String.format("% 10d", endOffset) + "\t"
                + " in:" + Util.getTimeString(rotation) + "\t"
                + recordTs + "]";
    }
}

package com.dp.blackhole.agent.persist;

import java.io.Serializable;

import com.dp.blackhole.common.Util;

public class Record implements Serializable{
    private static final long serialVersionUID = 6803400542870701190L;
    
    public static final int RESUME = 1;
    public static final int ROLL = 2;
    public static final int COMMIT  = 3;
    public static final int ROTATE  = 4;
    public static final int END     = 5;
    
    private final int type;
    private final long rollTs;
    private final long startOffset;
    private final long endOffset;
    private final long recordTs;
    
    public Record(int type, long rollTs, long startOffset, long endOffset) {
        this.type = type;
        this.rollTs = rollTs;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
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
    
    public String getTypeName() {
        String name;
        switch (this.type) {
        case RESUME:
            name = "RESUME";
            break;
        case ROLL:
            name = "ROLL";
            break;
        case COMMIT:
            name = "COMMIT";
            break;
        case ROTATE:
            name = "ROTATE";
            break;
        case END:
            name = "END";
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
                + recordTs + "]";
    }
}

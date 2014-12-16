package com.dp.blackhole.agent.persist;

public class Record implements Comparable<Record>{
    public static final int ATTEMPT = 1;
    public static final int COMMIT  = 2;
    public static final int ROTATE  = 3;
    public static final int END     = 4;
    
    private int type;
    private long offset;
    private long timestamp;
    public int getType() {
        return type;
    }
    public void setType(int type) {
        this.type = type;
    }
    public long getOffset() {
        return offset;
    }
    public void setOffset(long offset) {
        this.offset = offset;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public String getTypeName() {
        String name;
        switch (this.type) {
        case ATTEMPT:
            name = "ATTEMPT";
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
    public int compareTo(Record other) {
        if (this.offset - other.offset > 0) {
            return 1;
        } else if (this.offset - other.offset < 0) {
            return -1;
        }
        return 0;
    }
}

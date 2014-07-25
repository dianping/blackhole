package com.dp.blackhole.supervisor;

import java.util.List;

import com.dp.blackhole.common.Util;

public class Stage {
    public static final int APPENDING = 1;
    public static final int UPLOADING = 2;
    public static final int UPLOADED = 3;
    public static final int RECOVERYING = 4;
//    public static final int APPFAIL = 5;
    public static final int BROKERFAIL = 6;
//    public static final int NOBROKER = 7;
    public static final int PENDING = 5;
    
    List<Issue> issuelist;
    
    String topic;
    String sourceIdentify;
    String brokerhost;
    boolean cleanstart;
    int status;
    long rollTs;
    boolean isCurrent;
    
    private String getStatusString(int status) {
        switch (status) {
        case Stage.APPENDING:
            return "APPENDING";
        case Stage.UPLOADING:
            return "UPLOADING";
        case Stage.UPLOADED:
            return "UPLOADED";
        case Stage.RECOVERYING:
            return "RECOVERYING";
        case Stage.BROKERFAIL:
            return "BROKERFAIL";
        case Stage.PENDING:
            return "PENDING";
        default:
            return "UNKNOWN";
        }
    }
    
    public String toString() {
        String summary = topic + "@" + sourceIdentify + "," + getStatusString(status) + "," + Util.formatTs(rollTs);
        if (!cleanstart) {
            summary = summary + ",not cleanstart";
        }
        summary = summary + "\n";
        if (issuelist.size() != 0) {
            for(Issue i : issuelist) {
                summary = summary + i.toString();
            }
        }
        return summary;
    }

    public boolean isCurrent() {
        return isCurrent;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((sourceIdentify == null) ? 0 : sourceIdentify.hashCode());
        result = prime * result + (int) (rollTs ^ (rollTs >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Stage other = (Stage) obj;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        if (sourceIdentify == null) {
            if (other.sourceIdentify != null)
                return false;
        } else if (!sourceIdentify.equals(other.sourceIdentify))
            return false;
        if (rollTs != other.rollTs)
            return false;
        return true;
    }
}

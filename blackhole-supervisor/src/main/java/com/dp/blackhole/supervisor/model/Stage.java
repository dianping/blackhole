package com.dp.blackhole.supervisor.model;

import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.dp.blackhole.common.Util;

public class Stage {
    public static final int APPENDING = 1;
    public static final int UPLOADING = 2;
    public static final int UPLOADED = 3;
    public static final int RECOVERYING = 4;
    public static final int PENDING = 5;
    public static final int BROKERFAIL = 6;
    
    private List<Issue> issuelist;
    private String latestIssue;
    
    private String topic;
    private String source;
    private String brokerhost;
    private boolean cleanstart;
    private int status;
    private long rollTs;
    private boolean isCurrent;
    
    @JsonIgnore
    public List<Issue> getIssuelist() {
        return issuelist;
    }

    @JsonIgnore
    public void setIssuelist(List<Issue> issuelist) {
        this.issuelist = issuelist;
    }

    public String getLatestIssue() {
        if (issuelist.size() != 0) {
            this.latestIssue = issuelist.get(issuelist.size() - 1).toString();
        } else {
            this.latestIssue = null;
        }
        return latestIssue;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getBrokerhost() {
        return brokerhost;
    }

    public void setBrokerhost(String brokerhost) {
        this.brokerhost = brokerhost;
    }

    public boolean isCleanstart() {
        return cleanstart;
    }

    public void setCleanstart(boolean cleanstart) {
        this.cleanstart = cleanstart;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public long getRollTs() {
        return rollTs;
    }

    public void setRollTs(long rollTs) {
        this.rollTs = rollTs;
    }

    public boolean isCurrent() {
        return isCurrent;
    }
    
    public void setCurrent(boolean isCurrent) {
        this.isCurrent = isCurrent;
    }

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
        String summary = topic + "@" + source + "," + getStatusString(status) + "," + Util.formatTs(rollTs);
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((source == null) ? 0 : source.hashCode());
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
        if (source == null) {
            if (other.source != null)
                return false;
        } else if (!source.equals(other.source))
            return false;
        if (rollTs != other.rollTs)
            return false;
        return true;
    }
}

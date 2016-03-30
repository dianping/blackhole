package com.dp.blackhole.supervisor.model;

import java.text.SimpleDateFormat;
import java.util.Date;

public class LogNotFoundEntity {
    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final String topic;
    private final String host;
    private String file;
    private long ts;
    
    public LogNotFoundEntity(String topic, String host) {
        this.topic = topic;
        this.host = host;
    }

    public String getTopic() {
        return topic;
    }
    public String getHost() {
        return host;
    }
    public String getFile() {
        return file;
    }
    public void setFile(String file) {
        this.file = file;
    }
    public long getTs() {
        return ts;
    }
    public void setTs(long ts) {
        this.ts = ts;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
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
        LogNotFoundEntity other = (LogNotFoundEntity) obj;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return "LogNotFoundEntity [topic=" + topic + ", host=" + host
                + ", file=" + file + ", ts=" + format.format(new Date(ts)) + "]";
    }
}

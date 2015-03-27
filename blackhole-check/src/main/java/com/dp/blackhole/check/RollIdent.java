package com.dp.blackhole.check;

import java.util.ArrayList;
import java.util.List;

public class RollIdent {
    public String topic;
    public long period;
    public List<String> kvmSources = new ArrayList<String>();
    public List<String> paasSources = new ArrayList<String>();
    public long ts;
    public int timeout;
    public List<String> cmdbapp;
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
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
        RollIdent other = (RollIdent) obj;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return "RollIdent [topic=" + topic + ", period=" + period + ", kvmsources="
                + kvmSources + ", paasSources=" + paasSources + ", ts=" + ts + ", cmdbapp=" +cmdbapp + "]";
    }
}

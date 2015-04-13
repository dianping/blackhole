package com.dp.blackhole.broker;

public class RollIdent {
    public String topic;
    public long period;
    public String source;
    public long ts;
    public boolean isFinal;
    public boolean isPersist;
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (isFinal ? 1231 : 1237);
        result = prime * result + (int) (period ^ (period >>> 32));
        result = prime * result
                + ((source == null) ? 0 : source.hashCode());
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + (int) (ts ^ (ts >>> 32));
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
        if (isFinal != other.isFinal)
            return false;
        if (period != other.period)
            return false;
        if (source == null) {
            if (other.source != null)
                return false;
        } else if (!source.equals(other.source))
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        if (ts != other.ts)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return topic + "@" + source + "," + period + "," + ts + ",final:" + isFinal + ",persist:" + isPersist; 
    }
}

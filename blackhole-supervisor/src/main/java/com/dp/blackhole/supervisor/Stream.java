package com.dp.blackhole.supervisor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.dp.blackhole.common.Util;

public class Stream {
    String topic;
    String sourceIdentify;
    private String brokerHost;
    long period;
    long startTs;
    AtomicLong lastSuccessTs = new AtomicLong();
    AtomicBoolean active = new AtomicBoolean(true);
    
    synchronized void setBrokerHost(String newBrokerHost) {
        brokerHost = newBrokerHost;
    }
    
    synchronized String getBrokerHost() {
        return brokerHost;
    }
    
    void setlastSuccessTs(long ts) {
        lastSuccessTs.getAndSet(ts);
    }
    
    void setGreatlastSuccessTs(long ts) {
        for (;;) {
            long current = lastSuccessTs.get();
            if (ts > current) {
                if (lastSuccessTs.compareAndSet(current, ts)) {
                    break;
                }
            } else {
                break;
            }
        }
    }
    
    long getlastSuccessTs() {
        return lastSuccessTs.get();
    }
    
    boolean isActive() {
        return active.get();
    }
    
    void updateActive(boolean status) {
        active.getAndSet(status);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((sourceIdentify == null) ? 0 : sourceIdentify.hashCode());
        result = prime * result + (int) (period ^ (period >>> 32));
        result = prime * result + (int) (startTs ^ (startTs >>> 32));
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
        Stream other = (Stream) obj;
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
        if (period != other.period)
            return false;
        if (startTs != other.startTs)
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return topic+"@"+sourceIdentify+",period:" + period+ ",starttime:"+Util.formatTs(startTs)+",lastSuccessTs:"+lastSuccessTs.get()+",isActive:"+isActive();
    } 
}

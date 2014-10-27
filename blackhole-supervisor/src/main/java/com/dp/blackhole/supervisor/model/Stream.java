package com.dp.blackhole.supervisor.model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.dp.blackhole.common.Util;

public class Stream {
    private String topic;
    private String source;
    private String brokerHost;
    private long period;
    private long startTs;
    private AtomicLong lastSuccessTs = new AtomicLong();
    private AtomicBoolean active = new AtomicBoolean(true);
    private List<Stage> stages = new ArrayList<Stage>();
    
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

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public long getStartTs() {
        return startTs;
    }

    public void setStartTs(long startTs) {
        this.startTs = startTs;
    }

    public synchronized void setBrokerHost(String newBrokerHost) {
        brokerHost = newBrokerHost;
    }
    
    public synchronized String getBrokerHost() {
        return brokerHost;
    }
    
    @JsonIgnore
    public List<Stage> getStages() {
        return stages;
    }
    
    @JsonIgnore
    public void setStages(List<Stage> stageList) {
        this.stages = stageList;
    }
    
    public void updateLastSuccessTs(long ts) {
        lastSuccessTs.getAndSet(ts);
    }
    
    public void setGreatlastSuccessTs(long ts) {
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
    
    public long getLastSuccessTs() {
        return lastSuccessTs.get();
    }
    
    public boolean isActive() {
        return active.get();
    }
    
    public void updateActive(boolean status) {
        active.getAndSet(status);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((source == null) ? 0 : source.hashCode());
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
        if (source == null) {
            if (other.source != null)
                return false;
        } else if (!source.equals(other.source))
            return false;
        if (period != other.period)
            return false;
        if (startTs != other.startTs)
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return topic+"@"+source+",period:" + period+ ",starttime:"+Util.formatTs(startTs)+",lastSuccessTs:"+lastSuccessTs.get()+",isActive:"+isActive();
    }
}

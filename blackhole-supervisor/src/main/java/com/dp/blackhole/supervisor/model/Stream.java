package com.dp.blackhole.supervisor.model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.dp.blackhole.common.Util;

public class Stream {
    private StreamId streamId;
    private String brokerHost;
    private long period;
    private long startTs;
    private AtomicLong lastSuccessTs;
    private AtomicBoolean active;
    private List<Stage> stages;
    
    public Stream(String topic, String partition) {
        this.streamId = new StreamId(topic, partition);
        this.lastSuccessTs = new AtomicLong();
        this.active = new AtomicBoolean(true);
        this.stages = new ArrayList<Stage>();
    }
    
    public String getTopic() {
        return streamId.topic;
    }

    public String getSource() {
        return streamId.partition;
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
    
    public void setGreatlastSuccessTs(long ts) { //TODO lastSuccessTs should set step by step
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
        result = prime * result + (int) (period ^ (period >>> 32));
        result = prime * result + (int) (startTs ^ (startTs >>> 32));
        result = prime * result
                + ((streamId == null) ? 0 : streamId.hashCode());
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
        if (period != other.period)
            return false;
        if (startTs != other.startTs)
            return false;
        if (streamId == null) {
            if (other.streamId != null)
                return false;
        } else if (!streamId.equals(other.streamId))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return streamId + ",period:" + period+ ",starttime:"+Util.formatTs(startTs)+",lastSuccessTs:"+lastSuccessTs.get()+",isActive:"+isActive();
    }
    
    public static class StreamId {
        private final String topic;
        private final String partition;

        public StreamId(String topic, String partition) {
            this.topic = topic;
            this.partition = partition;
        }

        public String getTopic() {
            return topic;
        }

        public String getPartition() {
            return partition;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((partition == null) ? 0 : partition.hashCode());
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
            StreamId other = (StreamId) obj;
            if (partition == null) {
                if (other.partition != null)
                    return false;
            } else if (!partition.equals(other.partition))
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
            return topic + "@" + partition;
        }
    }
}

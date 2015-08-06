package com.dp.blackhole.datachecker;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.dp.blackhole.supervisor.model.Stream;

public class StreamsStatus implements Serializable {
    private static final long serialVersionUID = 1L;
    private Map<StreamStatusKey, StreamStatusValue> map;
    private Lock wLock;
    private Lock rLock;
    
    public StreamsStatus() {
        wLock = new ReentrantReadWriteLock().writeLock();
        rLock = new ReentrantReadWriteLock().readLock();
        map = new HashMap<StreamStatusKey, StreamStatusValue>(100);
    }
    
    public boolean update(Stream stream) {
        return update(stream.getTopic(), stream.getSource(), stream.getLastSuccessTs());
    }
    
    public boolean update(String topic, String source, long lastSuccessTs) {
        StreamStatusKey key = new StreamStatusKey(topic, source);
        StreamStatusValue value = new StreamStatusValue(lastSuccessTs);
        StreamStatusValue oldValue = get(key);
        if (oldValue == null || oldValue != value) {
            put(key, value);
            return true;
        }
        return false;
    }
    
    public long getLastSuccessTs(Stream stream) {
        return getLastSuccessTs(stream.getTopic(), stream.getSource());
    }
    
    public long getLastSuccessTs(String topic, String source) {
        StreamStatusKey key = new StreamStatusKey(topic, source);
        StreamStatusValue value = get(key);
        if (value == null) {
            return 0L;
        } else {
            return value.getLastSuccessTs();
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("StreamsStatus: \n");
        for (Map.Entry<StreamStatusKey, StreamStatusValue> entry : map.entrySet()) {
            sb.append("Key: ").append(entry.getKey()).append(" ,Value: ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    private StreamStatusValue get(StreamStatusKey key) {
        rLock.lock();
        try {
            return map.get(key);
        } finally {
            rLock.unlock();
        }
    }
    
    private void put(StreamStatusKey key, StreamStatusValue value) {
        wLock.lock();
        try {
            map.put(key, value);
        } finally {
            wLock.unlock();
        }
    }

    class StreamStatusKey implements Serializable {
        private static final long serialVersionUID = 1L;
        private String topic;
        private String source;
        public StreamStatusKey(String topic, String source) {
            this.topic = topic;
            this.source = source;
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
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result
                    + ((source == null) ? 0 : source.hashCode());
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
            StreamStatusKey other = (StreamStatusKey) obj;
            if (!getOuterType().equals(other.getOuterType()))
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
            return true;
        }
        private StreamsStatus getOuterType() {
            return StreamsStatus.this;
        }
        @Override
        public String toString() {
            return "[topic=" + topic + ", source=" + source + "]";
        }
    }
    
    class StreamStatusValue implements Serializable {
        private static final long serialVersionUID = 1L;
        private long lastSuccessTs;
        public StreamStatusValue(long lastSuccessTs) {
            this.lastSuccessTs = lastSuccessTs;
        }
        public long getLastSuccessTs() {
            return lastSuccessTs;
        }
        public void setLastSuccessTs(long lastSuccessTs) {
            this.lastSuccessTs = lastSuccessTs;
        }
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result
                    + (int) (lastSuccessTs ^ (lastSuccessTs >>> 32));
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
            StreamStatusValue other = (StreamStatusValue) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (lastSuccessTs != other.lastSuccessTs)
                return false;
            return true;
        }
        private StreamsStatus getOuterType() {
            return StreamsStatus.this;
        }
        @Override
        public String toString() {
            return "[lastSuccessTs=" + lastSuccessTs + "]";
        }
    }
}

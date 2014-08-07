package com.dp.blackhole.agent;

import java.util.concurrent.atomic.AtomicBoolean;

public class TopicMeta {
    private final MetaKey metaKey;
    private final String tailFile;
    private final long createTime;
    private final long rollPeriod;
    private final int maxLineSize;
    private AtomicBoolean dying;
    
    public TopicMeta(MetaKey metaKey, String tailFile, long rollPeriod, int maxLineSize) {
        this.metaKey = metaKey;
        this.tailFile = tailFile;
        this.rollPeriod = rollPeriod;
        this.createTime = System.currentTimeMillis();
        this.maxLineSize = maxLineSize;
        this.dying.set(false);
    }

    public MetaKey getMetaKey() {
        return metaKey;
    }
    
    public String getTopic() {
        return metaKey.getTopic();
    }
    
    public String getInstanceId() {
        return metaKey.getInstanceId();
    }

    public long getRollPeriod() {
        return rollPeriod;
    }

    public String getTailFile() {
        return tailFile;
    }

    public long getCreateTime() {
        return createTime;
    }

    public int getMaxLineSize() {
        return maxLineSize;
    }

    public boolean isDying() {
        return dying.get();
    }

    public boolean setDying() {
        return dying.compareAndSet(false, true);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (createTime ^ (createTime >>> 32));
        result = prime * result + maxLineSize;
        result = prime * result + ((metaKey == null) ? 0 : metaKey.hashCode());
        result = prime * result + (int) (rollPeriod ^ (rollPeriod >>> 32));
        result = prime * result
                + ((tailFile == null) ? 0 : tailFile.hashCode());
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
        TopicMeta other = (TopicMeta) obj;
        if (createTime != other.createTime)
            return false;
        if (maxLineSize != other.maxLineSize)
            return false;
        if (metaKey == null) {
            if (other.metaKey != null)
                return false;
        } else if (!metaKey.equals(other.metaKey))
            return false;
        if (rollPeriod != other.rollPeriod)
            return false;
        if (tailFile == null) {
            if (other.tailFile != null)
                return false;
        } else if (!tailFile.equals(other.tailFile))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "TopicMeta [metaKey=" + metaKey + ", tailFile=" + tailFile
                + ", createTime=" + createTime + ", rollPeriod=" + rollPeriod
                + ", maxLineSize=" + maxLineSize + "]";
    }

    static class MetaKey {

        private String topic;
        private String instanceId;
        
        public MetaKey(String topic, String instanceId) {
            this.topic = topic;
            this.instanceId = instanceId;
        }
        
        public String getTopic() {
            return topic;
        }

        public String getInstanceId() {
            return instanceId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((instanceId == null) ? 0 : instanceId.hashCode());
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
            MetaKey other = (MetaKey) obj;
            if (instanceId == null) {
                if (other.instanceId != null)
                    return false;
            } else if (!instanceId.equals(other.instanceId))
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
            String result = topic;
            if (instanceId != null) {
                result = result + "#" + instanceId;
            }
            return result;
        }
    }
}

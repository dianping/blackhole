package com.dp.blackhole.agent;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

import com.dp.blackhole.common.TopicCommonMeta;
import com.dp.blackhole.common.Util;

public class AgentMeta extends TopicCommonMeta implements Serializable {
    private static final long serialVersionUID = -1189314942488288669L;
    private final TopicId topicId;
    private final String source;
    private final String tailFile;
    private final long createTime;
    private final long rotatePeriod;
    private AtomicBoolean dying;
    private final long readInterval;
    private int bandwidthPerSec;
    private long tailPosition;
    
    public AgentMeta(TopicId topicId, String tailFile, long rotatePeriod,
            long rollPeriod, int maxLineSize, long readInterval,
            int minMsgSent, int msgBufSize, int bandwidthPerSec,
            int partitionFactor, long tailPosition) {
        super(rollPeriod, maxLineSize, minMsgSent, msgBufSize, partitionFactor);
        this.topicId = topicId;
        this.source = Util.getSource(Util.getLocalHost(), topicId.getInstanceId());
        this.tailFile = tailFile;
        this.rotatePeriod = rotatePeriod;
        this.createTime = System.currentTimeMillis();
        this.dying = new AtomicBoolean(false);
        this.readInterval = readInterval;
        this.bandwidthPerSec = bandwidthPerSec;
        this.tailPosition = tailPosition;
    }

    public TopicId getTopicId() {
        return topicId;
    }
    
    public String getTopic() {
        return topicId.getTopic();
    }
    
    public String getSource() {
        return source;
    }

    public long getRotatePeriod() {
        return rotatePeriod;
    }

    public String getTailFile() {
        return tailFile;
    }

    public long getCreateTime() {
        return createTime;
    }

    public boolean isDying() {
        return dying.get();
    }

    public boolean setDying() {
        return dying.compareAndSet(false, true);
    }

    public long getReadInterval() {
        return readInterval;
    }

    public int getBandwidthPerSec() {
        return bandwidthPerSec;
    }

    public void setBandwidthPerSec(int bandwidthPerSec) {
        this.bandwidthPerSec = bandwidthPerSec;
    }

    public long getTailPosition() {
        return tailPosition;
    }

    public void setTailPosition(long tailPosition) {
        this.tailPosition = tailPosition;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topicId == null) ? 0 : topicId.hashCode());
        result = prime * result + ((tailFile == null) ? 0 : tailFile.hashCode());
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
        AgentMeta other = (AgentMeta) obj;
        if (topicId == null) {
            if (other.topicId != null)
                return false;
        } else if (!topicId.equals(other.topicId))
            return false;
        if (rotatePeriod != other.rotatePeriod)
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
        return "TopicMeta [topicId=" + topicId + ", tailFile=" + tailFile
                + ", createTime=" + createTime + ", rotatePeriod="
                + rotatePeriod + ", rollPeriod=" + super.getRollPeriod()
                + ", maxLineSize=" + super.getMaxLineSize() + ", dying=" + dying
                + ", readInterval=" + readInterval + ", minMsgSent="
                + super.getMinMsgSent() + ", msgBufSize=" + super.getMsgBufSize()
                + ", partitionFactor=" + super.getPartitionFactor()
                + ", tailPosition=" + tailPosition + "]";
    }

    public static class TopicId implements Serializable {
        private static final long serialVersionUID = -7754556493037207950L;
        
        private String topic;
        private String instanceId;
        
        public TopicId(String topic, String instanceId) {
            this.topic = topic;
            if (instanceId == null || instanceId.length() == 0) {
                this.instanceId = null;
            } else {
                this.instanceId = instanceId;
            }
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
            TopicId other = (TopicId) obj;
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
        
        public String getContent() {
            String result = topic;
            if (instanceId != null) {
                result = result + "#" + instanceId;
            }
            return result;
        }
    }
}

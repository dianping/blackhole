package com.dp.blackhole.supervisor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Topic {
    private final String topic;
    // key is source
    private final ConcurrentHashMap<String, Stream> streams;
    // key is partitionId, now it's equivalent to source
    private final ConcurrentHashMap<String, PartitionInfo> partitions;
    
    public Topic(String topic) {
        this.topic = topic;
        this.streams = new ConcurrentHashMap<String, Stream>();
        this.partitions = new ConcurrentHashMap<String, PartitionInfo>();
    }
    public String getTopic() {
        return topic;
    }
    
    public void addStream(String source, Stream stream) {
        streams.put(source, stream);
    }
    
    public void addStream(Stream stream) {
        streams.put(stream.getSource(), stream);
    }
    
    public Stream getStream(String source) {
        return streams.get(source);
    }
    
    public void removeStream(String source) {
        streams.remove(source);
    }
    
    public List<Stream> getAllStreamsOfCopy() {
        return new ArrayList<Stream>(streams.values());
    }
    
    public void addPartition(String partitionId, PartitionInfo pInfo) {
        partitions.put(partitionId, pInfo);
    }
    
    public PartitionInfo getPartition(String partitionId) {
        return partitions.get(partitionId);
    }
    
    public void removePartition(String partitionId) {
        partitions.remove(partitionId);
    }
    
    public List<PartitionInfo> getAllPartitionsOfCopy() {
        return new ArrayList<PartitionInfo>(partitions.values());
    }
    
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
        Topic other = (Topic) obj;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }
}

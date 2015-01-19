package com.dp.blackhole.agent.persist;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.dp.blackhole.agent.TopicMeta.TopicId;
import com.dp.blackhole.common.Util;

public class Snapshot implements Serializable {
    private static final long serialVersionUID = 6034321879136783566L;
    
    private TopicId topicId;
    private long lastModifyTime;
    private long lastCleanTime;
    private boolean valid;
    private boolean complete;
    private List<Record> records;
    public Snapshot(TopicId topicId) {
        this.topicId = topicId;
        this.records = new ArrayList<Record>();
    }
    public TopicId getTopicId() {
        return topicId;
    }
    public void setTopicId(TopicId topicId) {
        this.topicId = topicId;
    }
    public long getLastModifyTime() {
        return lastModifyTime;
    }
    public void setLastModifyTime(long lastModifyTime) {
        this.lastModifyTime = lastModifyTime;
    }
    public long getLastCleanTime() {
        return lastCleanTime;
    }
    public void setLastCleanTime(long lastCleanTime) {
        this.lastCleanTime = lastCleanTime;
    }
    public boolean isValid() {
        return valid;
    }
    public void setValid(boolean valid) {
        this.valid = valid;
    }
    public boolean isComplete() {
        return complete;
    }
    public void setComplete(boolean complete) {
        this.complete = complete;
    }
    public synchronized List<Record> getRecords() {
        return records;
    }
    public synchronized void addRecord(Record record) {
        this.records.add(record);
    }
    public synchronized void remove(Record record) {
        this.records.remove(record);
    }
    public synchronized void eliminateExpiredRecord() {
        if (lastCleanTime > Util.getTS(-1, TimeUnit.DAYS)) {
            //clean snapshot only when clean interval is longer than 3 days
            return;
        }
        lastCleanTime = Util.getTS();
        Iterator<Record> it = records.iterator();  
        while(it.hasNext()){  
            Record record = it.next();  
            if(record.getRollTs() < Util.getTS(-7, TimeUnit.DAYS)){  
                it.remove();  
            } else {
                break;
            }
        }  
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topicId == null) ? 0 : topicId.hashCode());
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
        Snapshot other = (Snapshot) obj;
        if (topicId == null) {
            if (other.topicId != null)
                return false;
        } else if (!topicId.equals(other.topicId))
            return false;
        return true;
    }
    @Override
    public synchronized String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Snapshot [topicId=").append(topicId)
        .append(", lastModify=").append(lastModifyTime)
        .append(", valid=").append(valid)
        .append(", complete=").append(complete).append("]\n");
        builder.append("Records:\n");
        for (Record record : records) {
            builder.append(record).append("\n");
        }
        return builder.toString();
    }
}

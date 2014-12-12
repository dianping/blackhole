package com.dp.blackhole.agent.persist;

import java.util.SortedSet;

import com.dp.blackhole.agent.TopicMeta.MetaKey;

public class Snapshot {
    private MetaKey metaKey;
    private String stage;
    private long lastModify;
    private boolean valid;
    private boolean complete;
    private SortedSet<Record> records;
    public Snapshot(MetaKey metaKey, String stage) {
        this.metaKey = metaKey;
        this.stage = stage;
    }
    public MetaKey getMetaKey() {
        return metaKey;
    }
    public void setMetaKey(MetaKey metaKey) {
        this.metaKey = metaKey;
    }
    public String getStage() {
        return stage;
    }
    public void setStage(String stage) {
        this.stage = stage;
    }
    public long getLastModify() {
        return lastModify;
    }
    public void setLastModify(long lastModify) {
        this.lastModify = lastModify;
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
    public SortedSet<Record> getRecords() {
        return records;
    }
    public void setRecords(SortedSet<Record> records) {
        this.records = records;
    }
    public void addRecord(Record record) {
        this.records.add(record);
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((metaKey == null) ? 0 : metaKey.hashCode());
        result = prime * result + ((stage == null) ? 0 : stage.hashCode());
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
        if (metaKey == null) {
            if (other.metaKey != null)
                return false;
        } else if (!metaKey.equals(other.metaKey))
            return false;
        if (stage == null) {
            if (other.stage != null)
                return false;
        } else if (!stage.equals(other.stage))
            return false;
        return true;
    }
    @Override
    public String toString() {
        return "Snapshot [metaKey=" + metaKey + ", stage=" + stage
                + ", lastModify=" + lastModify + ", valid=" + valid
                + ", complete=" + complete + ", records=" + records + "]";
    }
}

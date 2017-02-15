package com.dp.blackhole.broker.follower;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.storage.ByteBufferMessageSet;

public class ReplicaMeta {
    private long fetchId;
    private String brokerLeader;
    private long offset;
    private AtomicBoolean active;
    private Partition flusher;
    private int resendCounter;
    private int resendTime;
    private long resendDelay;

    public ReplicaMeta(String brokerLeader, Partition flusher, int resendTime, long resendDelay) {
        this.fetchId = 0L;
        this.brokerLeader = brokerLeader;
        this.offset = flusher.getEndOffset();
        this.flusher = flusher;
        this.active = new AtomicBoolean(true);
        this.resendCounter = 0;
        this.resendTime = resendTime;
        this.resendDelay = resendDelay;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public boolean isActive() {
        return this.active.get();
    }

    public void setActive(boolean active) {
        this.active.set(active);
    }

    public int getEntropy() {
        return this.flusher.getEntropy();
    }

    public void append(ByteBufferMessageSet messageSet) throws IOException {
        flusher.append(messageSet);
    }

    public void adjustOffset(long startOffset) throws IOException {
        flusher.cleanupSegments(Util.getTS(), 0);
        flusher.reInitSegment(startOffset);
        this.offset = startOffset;
    }

    public String getBrokerLeader() {
        return this.brokerLeader;
    }

    public void flush() throws IOException {
        flusher.flush();
    }

    public void truncate(long offset) throws IOException {
        flusher.truncate(offset);
    }

    public void updateResend() {
        this.resendCounter++;
    }

    public boolean whetherDelay() {
        if (resendCounter > resendTime) {
            return true;
        }
        return false;
    }

    public void initResend() {
        this.resendCounter = 0;
    }

    public long getResendDelay() {
        return this.resendDelay;
    }

    public void updateFetchId(long id) {
        this.fetchId = id;
    }

    public long getFetchId() {
        return this.fetchId;
    }
}

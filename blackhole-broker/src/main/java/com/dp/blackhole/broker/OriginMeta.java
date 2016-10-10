package com.dp.blackhole.broker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.storage.ByteBufferMessageSet;

public class OriginMeta {
    private final Log LOG = LogFactory.getLog(OriginMeta.class);

    private volatile boolean active;
    private String topic;
    private String partition;
    private final int REPLICA_MIN = 1;
    private String leader;
    private ConcurrentHashMap<String, SyncStatus> brokerSyncMap;
    private Partition flusher;
    private long leo;
    private double insync_threshold;
    private double insync_buffer;
    private int max_tolerance;

    private enum Status {
        INSYNC, INSYNC_READY, OUTOFSYNC, OUTOFSYNC_READY
    }

    public OriginMeta(String topic, String partition, String leader, Map<String, Boolean> followers, long initOffset,
            Partition flusher, double insync_threshold, double insync_buffer, int tolerance) {
        this.active = true;
        this.topic = topic;
        this.partition = partition;
        this.leader = leader;
        brokerSyncMap = new ConcurrentHashMap<String, SyncStatus>();
        brokerSyncMap.put(leader, new SyncStatus(Status.OUTOFSYNC, initOffset));
        for (String follower : followers.keySet()) {
            brokerSyncMap.put(follower,
                    new SyncStatus(followers.get(follower) ? Status.INSYNC : Status.OUTOFSYNC, -1L));
        }
        this.leo = calLeo();
        this.flusher = flusher;
        this.insync_threshold = insync_threshold;
        this.insync_buffer = insync_buffer;
        this.max_tolerance = tolerance;
    }

    public long getEndOffset() {
        return brokerSyncMap.get(leader).offset;
    }

    public void newMessage() {
        this.brokerSyncMap.get(leader).offset = flusher.getEndOffset();
    }

    public void initFollowerOffset(String follower) {
        setFollowerOffset(follower, this.flusher.getStartOffset());
    }

    public HashMap<String, Boolean> getStatusChange(String follower, long offset) {
        setFollowerOffset(follower, offset);
        HashMap<String, Boolean> re = checkSyncStatus(follower);
        this.leo = calLeo();
        return re;
    }

    public boolean containsFolower(String follower) {
        if (brokerSyncMap.containsKey(follower) && !follower.equals(leader)) {
            return true;
        } else {
            return false;
        }
    }

    public long getFollowerOffset(String follower) {
        return brokerSyncMap.get(follower).offset;
    }

    private HashMap<String, Boolean> checkSyncStatus(String exclusion) {
        HashMap<String, Boolean> re = new HashMap<String, Boolean>();
        long leaderOffset = brokerSyncMap.get(this.leader).offset;
        if (leaderOffset - leo == 0) {
            return re;
        }
        if (!brokerSyncMap.containsKey(exclusion)) {
            return re;
        }
        SyncStatus syncStatus = brokerSyncMap.get(exclusion);
        if (syncStatus.status == Status.INSYNC) {
            for (Entry<String, SyncStatus> followerStatus : brokerSyncMap.entrySet()) {
                if (followerStatus.getKey().equals(this.leader)) {
                    continue;
                }
                if (followerStatus.getValue().status == Status.INSYNC) {
                    if ((followerStatus.getValue().offset - leo)
                            / (leaderOffset - leo) <= (this.insync_threshold - this.insync_buffer)
                            && !followerStatus.getKey().equals(exclusion)) {
                        followerStatus.getValue().updateTolerance();
                        if (followerStatus.getValue().tolerance >= this.max_tolerance) {
                            followerStatus.getValue().status = Status.OUTOFSYNC_READY;
                            re.put(followerStatus.getKey(), false);
                        }
                    }
                    if ((followerStatus.getValue().offset - leo)
                            / (leaderOffset - leo) >= (this.insync_threshold + this.insync_buffer)) {
                        followerStatus.getValue().initTolerance();
                    }
                    continue;
                }
            }
        } else if (syncStatus.status == Status.OUTOFSYNC) {
            if ((syncStatus.offset - leo) / (leaderOffset - leo) >= (this.insync_threshold + this.insync_buffer)) {
                syncStatus.initTolerance();
                syncStatus.status = Status.INSYNC_READY;
                re.put(exclusion, true);
            }
        }
        return re;
    }

    public boolean isFollowerReadyInSync(String follower) {
        SyncStatus followerStatus = brokerSyncMap.get(follower);
        if (followerStatus.status == Status.INSYNC_READY) {
            return true;
        }
        if (followerStatus.status == Status.INSYNC) {
            LOG.warn("follower: " + follower + " has already been in sync for Topic: " + topic + ", Partition: "
                    + partition);
        }
        return false;
    }

    public void rollbackFollowerStatus(String follower) {
        SyncStatus syncStatus = brokerSyncMap.get(follower);
        if (syncStatus == null) {
            return;
        }
        if (syncStatus.status == Status.INSYNC_READY) {
            syncStatus.status = Status.OUTOFSYNC;
            return;
        }
        if (syncStatus.status == Status.OUTOFSYNC_READY) {
            syncStatus.status = Status.INSYNC;
            return;
        }
    }

    public boolean isFollowerReadyOutOfSync(String follower) {
        SyncStatus followerStatus = brokerSyncMap.get(follower);
        if (followerStatus.status == Status.OUTOFSYNC_READY) {
            return true;
        }
        if (followerStatus.status == Status.OUTOFSYNC) {
            LOG.warn("follower: " + follower + " has already been out of sync for Topic: " + topic + ", Partition: "
                    + partition);
        }
        return false;
    }

    public boolean checkFollowerStillInSync(String follower) {
        SyncStatus followerStatus = brokerSyncMap.get(follower);
        long leaderOffset = brokerSyncMap.get(this.leader).offset;
        if ((followerStatus.offset - leo) / (leaderOffset - leo) > (this.insync_threshold - this.insync_buffer)) {
            return true;
        }
        return false;
    }

    public boolean checkFollowerStillOutOfSync(String follower) {
        SyncStatus followerStatus = brokerSyncMap.get(follower);
        long leaderOffset = brokerSyncMap.get(this.leader).offset;
        if ((followerStatus.offset - leo) / (leaderOffset - leo) < (this.insync_threshold - this.insync_buffer)) {
            return true;
        }
        return false;
    }

    private boolean setFollowerOffset(String follower, long offset) {
        SyncStatus ss = brokerSyncMap.get(follower);
        if (ss != null) {
            ss.offset = offset;
            return true;
        }
        return false;
    }

    public void setFollowerSyncStatus(String follower, boolean status) {
        SyncStatus ss = brokerSyncMap.get(follower);
        if (ss == null) {
            return;
        }
        ss.status = (status ? Status.INSYNC : Status.OUTOFSYNC);
    }

    public boolean deactive() {
        if (this.active == true) {
            this.active = false;
            return true;
        }
        return false;
    }

    public boolean isActive() {
        return this.active;
    }

    public boolean setLeaderOffline() {
        if (this.leader != null && this.brokerSyncMap.get(leader).status != Status.OUTOFSYNC) {
            this.brokerSyncMap.get(leader).status = Status.OUTOFSYNC;
            return true;
        }
        return false;
    }

    public boolean setLeaderOnline() {
        if (this.leader != null && this.brokerSyncMap.get(leader).status != Status.INSYNC) {
            this.brokerSyncMap.get(leader).status = Status.INSYNC;
            return true;
        }
        return false;
    }

    public boolean isLeaderOnline() {
        if (this.leader != null && this.brokerSyncMap.get(leader).status == Status.INSYNC) {
            return true;
        }
        return false;
    }

    public void flush() throws IOException {
        flusher.flush();
    }

    public void append(ByteBufferMessageSet messageSet) throws IOException {
        flusher.append(messageSet);
    }

    public int getEntropy() {
        return this.flusher.getEntropy();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("OriginMeta {\n");
        sb.append("  Topic: " + topic + ", Partition: " + partition + ", leo: " + leo + ".\n");
        sb.append("  Leader@Offset@SyncStatus: " + leader + "@" + brokerSyncMap.get(leader).offset + "@"
                + brokerSyncMap.get(leader).status + "@" + brokerSyncMap.get(leader).tolerance + ".\n");
        for (String follower : brokerSyncMap.keySet()) {
            if (!follower.equals(leader)) {
                sb.append("  Follower@Offset@SyncStatus: " + follower + "@" + brokerSyncMap.get(follower).offset + "@"
                        + brokerSyncMap.get(follower).status + "@" + brokerSyncMap.get(follower).tolerance + ".\n");
            }
        }
        sb.append("  EndOffset: " + brokerSyncMap.get(leader).offset + ".\n");
        sb.append("}\n");
        return sb.toString();
    }

    private long calLeo() {
        long re = Long.MAX_VALUE;
        for (Entry<String, SyncStatus> entry : brokerSyncMap.entrySet()) {
            if (entry.getKey().equals(this.leader)) {
                continue;
            }
            SyncStatus ss = entry.getValue();
            if (ss.status != Status.OUTOFSYNC) {
                if (ss.offset < re) {
                    re = ss.offset;
                }
            }
        }
        return re;
    }

    public long getLeo() {
        return this.leo;
    }

    private class SyncStatus {
        private Status status;
        private long offset;
        private int tolerance;

        private SyncStatus(Status status, long offset) {
            this.status = status;
            this.offset = offset;
            this.tolerance = 0;
        }

        private void updateTolerance() {
            this.tolerance++;
        }

        private void initTolerance() {
            this.tolerance = 0;
        }
    }
}

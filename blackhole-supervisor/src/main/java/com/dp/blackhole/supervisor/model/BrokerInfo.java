package com.dp.blackhole.supervisor.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.cat.Cat;
import com.dp.blackhole.common.TopicPartitionKey;

public class BrokerInfo {
    private final Log LOG = LogFactory.getLog(BrokerInfo.class);

    private TopicPartitionKey tpKey;
    private volatile long offset;
    private volatile int entropy;
    private volatile BrokerStatus leader;
    private ConcurrentHashMap<String, BrokerStatus> followers = new ConcurrentHashMap<String, BrokerStatus>();
    private ConcurrentHashMap<String, Long> candidates = null;

    public BrokerInfo(TopicPartitionKey tpKey, String leader, ArrayList<String> followers) {
        this.tpKey = tpKey;
        this.leader = new BrokerStatus(leader, true);
        for (String follower : followers) {
            this.followers.put(follower, new BrokerStatus(follower, false));
        }
        this.entropy = 0;
        this.offset = 0L;
    }

    public String getTopic() {
        return this.tpKey.getTopic();
    }

    public String getPartition() {
        return this.tpKey.getPartition();
    }

    public String getLeader() {
        if (this.leader == null) {
            return null;
        } else {
            return this.leader.broker;
        }
    }

    public long getOffset() {
        return this.offset;
    }

    public HashMap<String, Boolean> getFollowers() {
        HashMap<String, Boolean> re = new HashMap<String, Boolean>();
        for (BrokerStatus bs : followers.values()) {
            re.put(bs.broker, bs.status);
        }
        return re;
    }

    private boolean setFollowerOffline(String follower) {
        // TODO not used by far, keep it for future
        for (BrokerStatus bs : followers.values()) {
            if (bs.broker.equals(follower) && bs.status == true) {
                bs.status = false;
                return true;
            }
        }
        return false;
    }

    private boolean setFollowerOnline(String follower) {
        // TODO not used by far, keep it for future
        for (BrokerStatus bs : followers.values()) {
            if (bs.broker.equals(follower) && bs.status == false) {
                bs.status = true;
                return true;
            }
        }
        return false;
    }

    public boolean setFollowerStatus(String follower, boolean status) {
        for (BrokerStatus bs : followers.values()) {
            if (bs.broker.equals(follower)) {
                bs.status = status;
                return true;
            }
        }
        Cat.logEvent("Supervisor follower doesn't match topic: " + tpKey.getTopic(), tpKey.getPartition());
        LOG.fatal(follower + " is not belong to brokerInfo: " + toString());
        return false;
    }

    public boolean getFollowerStatus(String follower) {
        BrokerStatus bs = followers.get(follower);
        if (bs != null) {
            return bs.status;
        } else {
            return false;
        }
    }

    public void switchLeader(String semiLeader, long offset) {
        if (this.leader != null) {
            Cat.logEvent("Broker leader should be null when switching leader topic: " + tpKey.getTopic(),
                    tpKey.getPartition());
            LOG.fatal("Broker leader should be null when switching leader." + "\n" + this.toString() + "\n"
                    + "force switch");
            addFollower(this.leader);
        }
        BrokerStatus tmp = followers.get(semiLeader);
        removeFollower(semiLeader);
        this.leader = tmp;
        this.offset = offset;
    }

    public void setLeaderOffline() {
        if (this.leader == null) {
            return;
        }
        if (this.leader.status) {
            this.leader.status = false;
        }
        addFollower(this.leader);
        this.leader = null;
    }

    public boolean containsFollower(String follower) {
        return followers.containsKey(follower);
    }

    public boolean isOneOfReplicas(String broker) {
        return containsFollower(broker) || this.leader.broker.equals(broker);
    }

    public int getEntropy() {
        return this.entropy;
    }

    public Set<String> startElection() {
        ConcurrentHashMap<String, Long> tmp = this.initCandidates();
        if (tmp == null) {
            return null;
        }
        this.updateEntropy();
        this.candidates = tmp;
        this.setLeaderOffline();
        return this.candidates.keySet();
    }

    public boolean isValidEntropy(int newEntropy) {
        if (newEntropy < entropy) {
            return false;
        }
        return true;
    }

    public void updateLeo(String candidate, long offset) {
        if (candidates == null) {
            LOG.error("couldn't update leo because there are no candidates" + "\n" + this.toString());
            return;
        }
        if (offset == -1L) {
            candidates.remove(candidate);
            return;
        }
        candidates.put(candidate, offset);
    }

    public boolean isElectionSupport() {
        if (candidates == null) {
            LOG.error("couldn't descide whether election is support because there are no candidates" + "\n"
                    + this.toString());
            return false;
        }
        if (candidates.isEmpty()) {
            candidates = null;
            return false;
        }
        return true;
    }

    public boolean getElectionResult() {
        if (!isElectionReady()) {
            return false;
        }
        long semiOffset = -1L;
        Entry<String, Long> semiLeaderInfo = null;
        for (Entry<String, Long> entry : candidates.entrySet()) {
            if (entry.getValue() > semiOffset) {
                semiOffset = entry.getValue();
                semiLeaderInfo = entry;
            }
        }
        if (semiLeaderInfo == null) {
            return false;
        }
        this.switchLeader(semiLeaderInfo.getKey(), semiLeaderInfo.getValue());
        this.candidates = null;
        return true;
    }

    private void updateEntropy() {
        this.entropy++;
    }

    private boolean isElectionReady() {
        if (candidates == null) {
            LOG.error("couldn't descide whether election is ready because there are no candidates" + "\n"
                    + this.toString());
            return false;
        }
        for (Entry<String, Long> entry : candidates.entrySet()) {
            if (entry.getValue() == -1L) {
                return false;
            }
        }
        return true;
    }

    private void removeFollower(String follower) {
        followers.remove(follower);
    }

    private void addFollower(BrokerStatus follower) {
        if (follower != null) {
            followers.put(follower.broker, follower);
        }
    }

    private ConcurrentHashMap<String, Long> initCandidates() {
        ConcurrentHashMap<String, Long> candidates = new ConcurrentHashMap<String, Long>();
        for (Entry<String, BrokerStatus> entry : followers.entrySet()) {
            if (entry.getValue().status) {
                candidates.put(entry.getKey(), -1L);
            }
        }
        if (candidates.isEmpty()) {
            return null;
        }
        return candidates;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{" + "\n");
        sb.append("  ").append("entropy: " + entropy).append("\n");
        sb.append("  ").append("topic: " + tpKey.getTopic()).append("\n");
        sb.append("  ").append("partition" + tpKey.getPartition()).append("\n");
        sb.append("  ").append("offset: " + offset).append("\n");
        sb.append("  ").append("leader: ").append(leader).append("\n");
        if (followers == null) {
            sb.append("  follwers: null" + "\n");
        } else {
            for (BrokerStatus bi : followers.values()) {
                sb.append("  ").append("follower: ").append(bi.broker).append("\n");
            }
        }
        if (candidates == null) {
            sb.append("  candidates: null" + "\n");
        } else {
            for (Entry<String, Long> entry : candidates.entrySet()) {
                sb.append("  ").append("candidate: ").append(entry.getKey()).append(", leo: ").append(entry.getValue());
            }
        }
        return sb.toString();
    }

    private class BrokerStatus {
        private final String broker;
        private volatile boolean status;

        private BrokerStatus(String broker, boolean status) {
            this.broker = broker;
            this.status = status;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(broker).append(", status" + status);
            return sb.toString();
        }
    }
}

package com.dp.blackhole.broker.follower;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.common.TopicPartitionKey;
import com.dp.blackhole.common.Util;

public class FollowerFetcherGroup {
    private String brokerLeader;
    private int brokerLeaderPort;
    private ArrayList<FetcherInfo> fetcherList = new ArrayList<FetcherInfo>();
    private int resendTime;
    private long resendDelay;

    public FollowerFetcherGroup(String brokerLeader, int brokerLeaderPort, int followerResendTime, long followerResendDelay) {
        this.brokerLeader = brokerLeader;
        this.brokerLeaderPort = brokerLeaderPort;
        this.resendTime = followerResendTime;
        this.resendDelay = followerResendDelay;
    }

    public void start() {
        ConcurrentHashMap<TopicPartitionKey, ReplicaMeta> tpKeyReplicaMeta = new ConcurrentHashMap<TopicPartitionKey, ReplicaMeta>();
        FollowerFetcher fFetcher = new FollowerFetcher(this.brokerLeader, this.brokerLeaderPort, Util.getLocalHost(),
                tpKeyReplicaMeta);
        fFetcher.start();
        fetcherList.add(new FetcherInfo(fFetcher, tpKeyReplicaMeta));
    }

    public void addReplica(TopicPartitionKey tpKey, Partition p) {
        // TODO determin whether we should init a new fetcher and choose the
        // right fetcher
        for (FetcherInfo fFetcher : fetcherList) {
            if (fFetcher.followerFetcher != null) {
                fFetcher.followerFetcher.addReplica(tpKey.getTopic(), tpKey.getPartition(), p, resendTime, resendDelay);
            }
        }
    }

    public boolean containsReplica(TopicPartitionKey tpKey) {
        for (FetcherInfo fFetcher : fetcherList) {
            if (fFetcher.tpKeyReplicaMeta.contains(tpKey)) {
                return true;
            }
        }
        return false;
    }

    public ReplicaMeta getReplicaMeta(TopicPartitionKey tpKey) {
        ReplicaMeta rm = null;
        for (FetcherInfo fFetcher : fetcherList) {
            rm = fFetcher.tpKeyReplicaMeta.get(tpKey);
            if (rm != null) {
                break;
            }
        }
        return rm;
    }

    protected boolean removeReplica(TopicPartitionKey tpKey) {
        boolean re = false;
        for (FetcherInfo fFetcher : fetcherList) {
            if (fFetcher.tpKeyReplicaMeta.containsKey(tpKey)) {
                fFetcher.tpKeyReplicaMeta.remove(tpKey);
                re = true;
                break;
            }
        }
        return re;
    }

    private class FetcherInfo {
        private FollowerFetcher followerFetcher;
        private ConcurrentHashMap<TopicPartitionKey, ReplicaMeta> tpKeyReplicaMeta;

        private FetcherInfo(FollowerFetcher followerFetcher,
                ConcurrentHashMap<TopicPartitionKey, ReplicaMeta> tpKeyReplicaMeta) {
            this.followerFetcher = followerFetcher;
            this.tpKeyReplicaMeta = tpKeyReplicaMeta;
        }
    }
}

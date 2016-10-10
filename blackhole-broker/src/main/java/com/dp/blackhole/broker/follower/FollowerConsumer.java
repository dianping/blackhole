package com.dp.blackhole.broker.follower;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.StorageManager;
import com.dp.blackhole.common.TopicPartitionKey;

public class FollowerConsumer {
    private final Log LOG = LogFactory.getLog(FollowerConsumer.class);

    private StorageManager storageManager;
    private ConcurrentHashMap<String, FollowerFetcherGroup> leaderFetcherGroup = new ConcurrentHashMap<String, FollowerFetcherGroup>();

    private int followerResendTime;
    private long followerResendDelay;

    public FollowerConsumer(StorageManager storageManager, Properties prop) {
        this.storageManager = storageManager;
        this.followerResendTime = Integer.parseInt(prop.getProperty("broker.follower.resend.time", "3"));
        this.followerResendDelay = Long.parseLong(prop.getProperty("broker.follower.resend.delay", "1"));
    }

    public void stopFollowerIfExists(TopicPartitionKey tpKey) throws IOException {
        ReplicaMeta rm = null;
        for (Entry<String, FollowerFetcherGroup> ffg : leaderFetcherGroup.entrySet()) {
            rm = ffg.getValue().getReplicaMeta(tpKey);
            if (rm == null) {
                continue;
            }
            synchronized (rm) {
                rm.flush();
                rm.setActive(false);
                ffg.getValue().removeReplica(tpKey);
            }
        }
    }

    public long getLeo(TopicPartitionKey tpKey) {
        ReplicaMeta rm = null;
        for (FollowerFetcherGroup ffg : leaderFetcherGroup.values()) {
            rm = ffg.getReplicaMeta(tpKey);
            if (rm == null) {
                continue;
            }
            return rm.getOffset();
        }
        return -1;
    }

    public void addReplica(String brokerLeader, int brokerLeaderPort, TopicPartitionKey tpKey, Partition p) {
        FollowerFetcherGroup ffg = leaderFetcherGroup.get(brokerLeader);
        ReplicaMeta rm = null;
        for (Entry<String, FollowerFetcherGroup> ffgEntry : leaderFetcherGroup.entrySet()) {
            rm = ffgEntry.getValue().getReplicaMeta(tpKey);
            if (rm == null || !rm.isActive()) {
                continue;
            }
            // TODO whether we can delete active prop and use
            // containsReplica()
            synchronized (rm) {
                if (!rm.getBrokerLeader().equals(brokerLeader)) {
                    rm.setActive(false);
                    ffgEntry.getValue().removeReplica(tpKey);
                }
                addReplica(ffg, brokerLeader, brokerLeaderPort, tpKey, p);
                return;
            }
        }
        addReplica(ffg, brokerLeader, brokerLeaderPort, tpKey, p);
    }

    private void addReplica(FollowerFetcherGroup ffg, String brokerLeader, int brokerLeaderPort,
            TopicPartitionKey tpKey, Partition p) {
        if (ffg != null) {
            ffg.addReplica(tpKey, p);
        } else {
            FollowerFetcherGroup newffg = new FollowerFetcherGroup(brokerLeader, brokerLeaderPort, followerResendTime, followerResendDelay);
            this.leaderFetcherGroup.put(brokerLeader, newffg);
            newffg.start();
            newffg.addReplica(tpKey, p);
        }
    }
}
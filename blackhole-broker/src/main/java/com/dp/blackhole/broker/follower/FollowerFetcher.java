package com.dp.blackhole.broker.follower;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.TopicPartitionKey;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.NioService;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.network.TransferWrapNonblockingConnection;
import com.dp.blackhole.protocol.data.DataMessageTypeFactory;
import com.dp.blackhole.protocol.data.ReplicaFetchRep;
import com.dp.blackhole.protocol.data.ReplicaFetchReq;
import com.dp.blackhole.storage.ByteBufferMessageSet;

public class FollowerFetcher extends Thread {
    private final Log LOG = LogFactory.getLog(FollowerFetcher.class);

    private String brokerLeader;
    private int brokerLeaderPort;
    private String localhost;
    private FFetcherProcessor ffp;
    private GenClient<TransferWrap, TransferWrapNonblockingConnection, FFetcherProcessor> client;
    private ConcurrentHashMap<TopicPartitionKey, ReplicaMeta> tpKeyReplicaMeta;
    private ScheduledExecutorService retryPool;

    public FollowerFetcher(String brokerLeader, int brokerLeaderPort, String localhost,
            ConcurrentHashMap<TopicPartitionKey, ReplicaMeta> tpKeyReplicaMeta) {
        this.brokerLeader = brokerLeader;
        this.brokerLeaderPort = brokerLeaderPort;
        this.localhost = localhost;
        ffp = new FFetcherProcessor();
        client = new GenClient<TransferWrap, TransferWrapNonblockingConnection, FFetcherProcessor>(ffp,
                new TransferWrapNonblockingConnection.TransferWrapNonblockingConnectionFactory(),
                new DataMessageTypeFactory());
        this.tpKeyReplicaMeta = tpKeyReplicaMeta;
    }

    public void addReplica(String topic, String partition, Partition p, int resendTime, long resendDelay) {
        TopicPartitionKey tpKey = new TopicPartitionKey(topic, partition);
        if (!tpKeyReplicaMeta.containsKey(tpKey)) {
            ReplicaMeta rm = new ReplicaMeta(brokerLeader, p, resendTime, resendDelay);
            synchronized (tpKeyReplicaMeta) {
                tpKeyReplicaMeta.put(tpKey, rm);
                if (ffp.connection != null) {
                    ffp.sendReplicaFetchReq(topic, partition, rm.getOffset(), rm.getEntropy(), 0L, 1);
                }
            }
        }
    }

    private boolean isEqualEntropy(int newEntropy, String topic, String partition, ReplicaMeta rm) {
        int oldEntropy = rm.getEntropy();
        LOG.debug("ValidReplicaReplyEntropy: compare new entropy: " + newEntropy + " and old entropy: " + oldEntropy
                + " for topic: " + topic + ", partition: " + partition);
        return newEntropy == oldEntropy ? true : false;
    }

    @Override
    public void run() {
        LOG.info("starting replica fetcher " + getName() + " to broker " + brokerLeader);
        try {
            client.init(getName(), brokerLeader, brokerLeaderPort);
        } catch (ClosedChannelException e) {
            LOG.error("ClosedChannelException catched: ", e);
        } catch (IOException e) {
            LOG.error("IOException catched: ", e);
        }
        LOG.debug("stopping replica fetcher " + getName() + " to broker " + brokerLeader);
    }

    class FFetcherProcessor implements EntityProcessor<TransferWrap, TransferWrapNonblockingConnection> {
        private volatile TransferWrapNonblockingConnection connection;

        @Override
        public void OnConnected(TransferWrapNonblockingConnection connection) {
            LOG.info("Fetcher " + this + " process connected with " + connection);
            retryPool = Executors.newSingleThreadScheduledExecutor();
            synchronized (tpKeyReplicaMeta) {
                this.connection = connection;
                Iterator<TopicPartitionKey> iter = tpKeyReplicaMeta.keySet().iterator();
                while (iter.hasNext()) {
                    TopicPartitionKey tpKey = iter.next();
                    ReplicaMeta rm = tpKeyReplicaMeta.get(tpKey);
                    sendReplicaFetchReq(tpKey.getTopic(), tpKey.getPartition(), rm.getOffset(), rm.getEntropy(), 0L, 1);
                }
            }
        }

        @Override
        public void OnDisconnected(TransferWrapNonblockingConnection connection) {
            retryPool.shutdown();
            retryPool = null;
            this.connection = null;
            LOG.info("Fetcher " + this + " disconnected but will reconnect to " + connection);
        }

        @Override
        public void receiveTimout(TransferWrap msg, TransferWrapNonblockingConnection conn) {

        }

        @Override
        public void sendFailure(TransferWrap msg, TransferWrapNonblockingConnection conn) {

        }

        @Override
        public void setNioService(NioService<TransferWrap, TransferWrapNonblockingConnection> service) {

        }

        @Override
        public void process(TransferWrap reply, TransferWrapNonblockingConnection from) {
            switch (reply.getType()) {
            case DataMessageTypeFactory.ReplicaFetchRep:
                handleReplicaFetchReply((ReplicaFetchRep) reply.unwrap(), from);
                break;
            default:
                LOG.error("response type is undefined");
                break;
            }
        }

        private void sendReplicaFetchReq(String topic, String partition, long offset, int entropy, long delay,
                long id) {
            ReplicaMeta rm = tpKeyReplicaMeta.get(new TopicPartitionKey(topic, partition));
            if (rm == null || !rm.isActive()) {
                return;
            }
            if (delay > 0L) {
                RetryTask rt = new RetryTask(this.connection, topic, partition, offset, entropy, id);
                retryPool.schedule(rt, delay, TimeUnit.MILLISECONDS);
                return;
            }
            if (connection != null) {
                LOG.debug("Fetcher: " + this + ", Send replica fetch request to Leader: " + brokerLeader + " Topic: "
                        + topic + ", Partition: " + partition + ", Offset: " + offset + ", FetchId: " + id);
                connection.send(new TransferWrap(new ReplicaFetchReq(entropy, topic, partition, brokerLeader, localhost,
                        offset, ParamsKey.TopicConf.DEFAULT_REPLICA_FETCHSIZE, id)));
            }
        }

        private void handleReplicaFetchReply(ReplicaFetchRep reply, TransferWrapNonblockingConnection from) {
            String topic = reply.getTopic();
            String partition = reply.getPartition();
            long offset = reply.getOffset();
            int size = reply.getSize();
            long id = reply.getId();
            LOG.debug("Fetcher: " + this + ", Received replica fetch reply Topic: " + topic + ", Partition: "
                    + partition + ", Offset: " + offset + ", Size: " + size + ", FetchId: " + id);
            TopicPartitionKey tpKey = new TopicPartitionKey(topic, partition);
            ReplicaMeta rm = tpKeyReplicaMeta.get(tpKey);
            if (rm == null) {
                return;
            }
            synchronized (rm) {
                if (!tpKeyReplicaMeta.containsKey(tpKey)) {
                    return;
                }
                if (!isEqualEntropy(reply.getEntropy(), topic, partition, rm)) {
                    return;
                }
                rm.updateFetchId(id + 1);
                if (rm.getOffset() != -1 && rm.getOffset() != offset) {
                    LOG.fatal("Need to adjust offset. Leader: " + reply.getBrokerLeader() + ", Topic: " + topic
                            + ", Partition: " + partition + ", oldOffset: " + rm.getOffset() + ", newOffset: " + offset
                            + ", Size: " + size + "\n"
                            + "Cleanup segments and create a new partition with StartOffset: " + offset);
                    try {
                        rm.adjustOffset(offset);
                    } catch (IOException e) {
                        LOG.error("IOE catched", e);
                        sendReplicaFetchReq(topic, partition, offset, rm.getEntropy(), 0L, rm.getFetchId());
                        return;
                    }
                }
                if (size == 0) {
                    rm.updateResend();
                    if (rm.whetherDelay()) {
                        sendReplicaFetchReq(topic, partition, rm.getOffset(), rm.getEntropy(), rm.getResendDelay(),
                                rm.getFetchId());
                        return;
                    }
                    sendReplicaFetchReq(topic, partition, rm.getOffset(), rm.getEntropy(), 0L, rm.getFetchId());
                } else {
                    rm.initResend();
                    try {
                        ByteBufferMessageSet messageSet = (ByteBufferMessageSet) reply.getMessageSet();
                        LOG.debug("Fetcher: " + this + ", Received replica fetch reply Topic: " + topic
                                + ", Partition: " + partition + ", Offset: " + offset + ", ValidSize: "
                                + messageSet.getValidSize() + ", FetchId: " + id);
                        messageSet.makeValid();
                        rm.append(messageSet);
                        long newOffset = rm.getOffset() + messageSet.getValidSize();
                        rm.setOffset(newOffset);
                        sendReplicaFetchReq(topic, partition, rm.getOffset(), rm.getEntropy(), 0L, rm.getFetchId());
                    } catch (IOException e) {
                        LOG.error("IOE catched", e);
                        sendReplicaFetchReq(topic, partition, rm.getOffset(), rm.getEntropy(), 0L, rm.getFetchId());
                    }
                }
            }
        }

        class RetryTask implements Runnable {
            private TransferWrapNonblockingConnection conn;
            private String topic;
            private String partition;
            private long offset;
            private int entropy;
            private long id;

            public RetryTask(TransferWrapNonblockingConnection conn, String topic, String partition, long offset,
                    int entropy, long id) {
                this.conn = conn;
                this.topic = topic;
                this.partition = partition;
                this.offset = offset;
                this.entropy = entropy;
                this.id = id;
            }

            @Override
            public void run() {
                if (this.conn != null) {
                    sendReplicaFetchReq(topic, partition, offset, entropy, 0L, id);
                }
            }
        }

    }
}

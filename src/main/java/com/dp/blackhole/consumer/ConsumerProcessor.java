package com.dp.blackhole.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.collectornode.persistent.ByteBufferMessageSet;
import com.dp.blackhole.collectornode.persistent.MessageAndOffset;
import com.dp.blackhole.collectornode.persistent.MessageSet;
import com.dp.blackhole.collectornode.persistent.protocol.DataMessageTypeFactory;
import com.dp.blackhole.collectornode.persistent.protocol.FetchReply;
import com.dp.blackhole.collectornode.persistent.protocol.FetchRequest;
import com.dp.blackhole.collectornode.persistent.protocol.MultiFetchReply;
import com.dp.blackhole.collectornode.persistent.protocol.MultiFetchRequest;
import com.dp.blackhole.collectornode.persistent.protocol.OffsetReply;
import com.dp.blackhole.collectornode.persistent.protocol.OffsetRequest;
import com.dp.blackhole.network.DelegationIOConnection;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.TransferWrap;

public class ConsumerProcessor implements EntityProcessor<TransferWrap, DelegationIOConnection> {
    private final Log LOG = LogFactory.getLog(ConsumerProcessor.class);
    private final Map<String, PartitionTopicInfo> partitionMap = new HashMap<String, PartitionTopicInfo>();
    private Map<PartitionTopicInfo, Boolean> partitionBlockMap = new HashMap<PartitionTopicInfo, Boolean>();
    private boolean betterOrdered;

    /**
     * @param partitionTopicInfos
     * @param betterOrdered if true, the latency of message receiving will be increase,
     * but messages receiving form different partitions are synchronous and better time ordered.
     */
    public ConsumerProcessor(List<PartitionTopicInfo> partitionTopicInfos, boolean betterOrdered) {
        this.betterOrdered = betterOrdered;
        for (PartitionTopicInfo info : partitionTopicInfos) {
            partitionMap.put(info.partition, info);
            partitionBlockMap.put(info, false);
        }
    }

    @Override
    public void OnConnected(DelegationIOConnection connection) {
        //Offset from supervisor less than zero means that it should be correct first. 
        //That's a specification rule.
        boolean offsetNeedRecoveryBeforeWork = false;
        for (PartitionTopicInfo info : partitionBlockMap.keySet()) {
            if (info.getFetchedOffset() < 0) {
                offsetNeedRecoveryBeforeWork = true;
                partitionBlockMap.put(info, true);
                sendOffsetRequest(connection, info);
            }
        }
        if (!offsetNeedRecoveryBeforeWork) {
            if (betterOrdered) {
                sendMultiFetchRequest(connection);
            } else {
                for (PartitionTopicInfo info : partitionBlockMap.keySet()) {
                    sendFetchRequest(connection, info);
                }
            }
        }
    }

    @Override
    public void OnDisconnected(DelegationIOConnection connection) {
        //TODO What should I do?
        partitionBlockMap.clear();
        partitionMap.clear();
    }

    @Override
    public void process(TransferWrap response, DelegationIOConnection from) {
        switch (response.getType()) {
        case DataMessageTypeFactory.FetchReply:
            handleFetchReply(response, from);
            break;
        case DataMessageTypeFactory.OffsetReply:
            handleOffsetReply(response, from);
            break;
        case DataMessageTypeFactory.MultiFetchReply:
            handleMultiFetchReply(response, from);
            break;
        default:
            LOG.error("response type is undefined");
            break;
        }
    }

    private void handleMultiFetchReply(TransferWrap response,
            DelegationIOConnection from) {
        MultiFetchReply multiFetchReply = (MultiFetchReply)response.unwrap();
        List<String> partitions = multiFetchReply.getPartitionList();
        List<MessageSet> messageSets = multiFetchReply.getMessagesList();
        List<Long> offsets = multiFetchReply.getOffsetList();
        for (int i = 0; i < partitions.size(); i++) {
            PartitionTopicInfo info = partitionMap.get(partitions.get(i));
            long offset = offsets.get(i);
            if (offset == MessageAndOffset.OFFSET_OUT_OF_RANGE) {
                partitionBlockMap.put(info, true);
                sendOffsetRequest(from, info);
            } else {
                try {
                    info.enqueue((ByteBufferMessageSet)messageSets.get(i), info.getFetchedOffset());
                } catch (InterruptedException e) {
                    LOG.error("Oops, catch an Interrupted Exception of queue.put()," +
                    		" but ignore it.", e);//TODO to be review;
                } catch (RuntimeException e) {
                    throw e;
                }
            }
        }
        if (!needBlocking()) {
            sendMultiFetchRequest(from);
        }
    }

    private void handleOffsetReply(TransferWrap response,
            DelegationIOConnection from) {
        OffsetReply offsetReply = (OffsetReply) response.unwrap();
        long resetOffset = offsetReply.getOffset();
        String topic = offsetReply.getTopic();
        String partition = offsetReply.getPartition();
        PartitionTopicInfo info = partitionMap.get(partition);
        if (resetOffset >= 0) {
            info.resetFetchOffset(resetOffset);
            info.resetConsumeOffset(resetOffset);
            partitionBlockMap.put(info, false);
        } else {
            LOG.warn("Reset offset incorrect! Retry!");
            sendOffsetRequest(from, info);
            return;
        }
        
        ConsumerConnector connector = ConsumerConnector.getInstance();
        connector.updateOffset(topic, partition, resetOffset);
        
        if (betterOrdered) {
            if (!needBlocking()) {
                sendMultiFetchRequest(from);
            }
        } else {
            sendFetchRequest(from, info);
        }
    }

    private void handleFetchReply(TransferWrap response,
            DelegationIOConnection from) {
        FetchReply fetchReply = (FetchReply) response.unwrap();
        ByteBufferMessageSet messageSet = (ByteBufferMessageSet) fetchReply.getMessageSet();
        String partition = fetchReply.getPartition();
        PartitionTopicInfo info = partitionMap.get(partition);
        long offset = fetchReply.getOffset();
        if (offset == MessageAndOffset.OFFSET_OUT_OF_RANGE) {
            sendOffsetRequest(from, info);
        } else {
            try {
                info.enqueue(messageSet, info.getFetchedOffset());
            } catch (InterruptedException e) {
                LOG.error("Oops, catch an Interrupted Exception of queue.put()," +
                		" but ignore it.", e);//TODO to be review;
            } catch (RuntimeException e) {
                throw e;
            }
        }
        sendFetchRequest(from, info);
    }

    private void sendMultiFetchRequest(DelegationIOConnection connection) {
        List<FetchRequest> fetches = new ArrayList<FetchRequest>();
        for (PartitionTopicInfo info : partitionBlockMap.keySet()) {
            fetches.add(
                new FetchRequest(
                    info.topic, 
                    info.partition, 
                    info.getFetchedOffset(), 
                    Consumer.config.getFetchSize()
                )
            );
        }
        connection.send(new TransferWrap(new MultiFetchRequest(fetches)));
    }

    private void sendOffsetRequest(DelegationIOConnection from,
            PartitionTopicInfo info) {
        LOG.info("offset for " + info + " out of range, now we fix it");
        long offset;
        String autoOffsetReset = Consumer.config.getAutoOffsetReset();
        if (OffsetRequest.SMALLES_TIME_STRING.equals(autoOffsetReset)) {
            offset = OffsetRequest.EARLIES_TTIME;
        } else if (OffsetRequest.LARGEST_TIME_STRING.equals(autoOffsetReset)) {
            offset = OffsetRequest.LATES_TTIME;
        } else {
            offset = OffsetRequest.LATES_TTIME;
        }
        from.send(new TransferWrap(new OffsetRequest(info.topic, info.partition, offset)));
    }

    private void sendFetchRequest(DelegationIOConnection from,
            PartitionTopicInfo info) {
        from.send(
            new TransferWrap(
                new FetchRequest(
                        info.topic, 
                        info.partition, 
                        info.getFetchedOffset(), 
                        Consumer.config.getFetchSize()
                )
            )
        );
    }

    private boolean needBlocking() {
        for(Map.Entry<PartitionTopicInfo, Boolean> entry : partitionBlockMap.entrySet()) {
            if (entry.getValue()) {
                return entry.getValue();
            }
        }
        return false;
    }
}

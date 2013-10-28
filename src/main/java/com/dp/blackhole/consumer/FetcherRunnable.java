package com.dp.blackhole.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.appnode.Appnode;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.gen.AssignConsumerPB.AssignConsumer.Broker;
import com.dp.blackhole.consumer.api.FetchRequest;
import com.dp.blackhole.consumer.message.ByteBufferMessageSet;

import static java.lang.String.format;

public class FetcherRunnable extends Thread {

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final SimpleConsumer simpleConsumer;

    private volatile boolean stopped = false;

    private final Broker broker;

    private final List<PartitionTopicInfo> partitionTopicInfos;

    private final Log logger = LogFactory.getLog(FetcherRunnable.class);
    
    public FetcherRunnable(Broker broker,//
                           List<PartitionTopicInfo> partitionTopicInfos) {
        this.broker = broker;
        this.partitionTopicInfos = partitionTopicInfos;
        this.simpleConsumer = new SimpleConsumer(broker.getHost(), broker.getPort());
    }

    public void shutdown() throws InterruptedException {
        logger.debug("shutdown the fetcher " + getName());
        stopped = true;
        interrupt();
        shutdownLatch.await(5, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        StringBuilder buf = new StringBuilder("[");
        for (PartitionTopicInfo pti : partitionTopicInfos) {
            buf.append(format("%s-%d-%d,", pti.topic, pti.partition.brokerId, pti.partition.partId));
        }
        buf.append(']');
        logger.info(String.format("%s comsume at %s:%d with %s", getName(), broker.getHost(), broker.getPort(), buf.toString()));
        //
        try {
            while (!stopped) {
                if (fetchOnce() == 0) {//read empty bytes
                    logger.debug("backing off 1s");
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            if (stopped) {
                logger.info("FetcherRunnable " + this + " interrupted");
            } else {
                logger.error("error in FetcherRunnable ", e);
            }
        }
        //
        logger.debug("stopping fetcher " + getName() + " to broker " + broker);
        simpleConsumer.close();
        shutdownComplete();
    }

    private long fetchOnce() throws IOException, InterruptedException {
        List<FetchRequest> fetches = new ArrayList<FetchRequest>();
        for (PartitionTopicInfo info : partitionTopicInfos) {
            fetches.add(new FetchRequest(info.topic, info.partition.partId, info.getFetchedOffset(),1024 * 1024));//1MB
        }
        MultiFetchResponse response = simpleConsumer.multifetch(fetches);
        int index = 0;
        long read = 0L;
        for (ByteBufferMessageSet messages : response) {
            PartitionTopicInfo info = partitionTopicInfos.get(index);
            //
            try {
                read += processMessages(messages, info);
            } catch (IOException e) {
                throw e;
            } catch (InterruptedException e) {
                if (!stopped) {
                    logger.error("error in FetcherRunnable for " + info, e);
                    info.enqueueError(e, info.getFetchedOffset());
                }
                throw e;
            } catch (RuntimeException e) {
                if (!stopped) {
                    logger.error("error in FetcherRunnable for " + info, e);
                    info.enqueueError(e, info.getFetchedOffset());
                }
                throw e;
            }

            //
            index++;
        }
        return read;
    }

    private long processMessages(ByteBufferMessageSet messages, PartitionTopicInfo info) throws IOException,
            InterruptedException {
        boolean done = false;
        if (messages.getErrorCode() == ErrorMapping.OffsetOutOfRangeCode) {
            logger.warn("offset for " + info + " out of range, now we fix it");
            long resetOffset = resetConsumerOffsets(info.topic, info.partition);
            if (resetOffset >= 0) {
                info.resetFetchOffset(resetOffset);
                info.resetConsumeOffset(resetOffset);
                done = true;
            }
        }
        if (!done) {
            return info.enqueue(messages, info.getFetchedOffset());
        }
        return 0;
    }

    private void shutdownComplete() {
        this.shutdownLatch.countDown();
    }

    private long resetConsumerOffsets(String topic, Partition partition) throws IOException {
        long offset = -1;
        String autoOffsetReset = config.getAutoOffsetReset();
        if (OffsetRequest.SMALLES_TIME_STRING.equals(autoOffsetReset)) {
            offset = OffsetRequest.EARLIES_TTIME;
        } else if (OffsetRequest.LARGEST_TIME_STRING.equals(autoOffsetReset)) {
            offset = OffsetRequest.LATES_TTIME;
        }
        //
        final ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(config.getGroupId(), topic);
        long[] offsets = simpleConsumer.getOffsetsBefore(topic, partition.partId, offset, 1);
        //TODO: fixed no offsets?
        ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + partition.getName(), "" + offsets[0]);
        return offsets[0];
    }
}

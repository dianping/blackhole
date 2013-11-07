package com.dp.blackhole.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.collectornode.persistent.ByteBufferMessageSet;
import com.dp.blackhole.collectornode.persistent.protocol.FetchRequest;
import com.dp.blackhole.collectornode.persistent.protocol.OffsetRequest;
import com.dp.blackhole.common.Util;

import static java.lang.String.format;

public class FetcherRunnable extends Thread {

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final SimpleConsumer simpleConsumer;

    private volatile boolean stopped = false;

    private final String broker;

    private final List<PartitionTopicInfo> partitionTopicInfos;

    private final Log logger = LogFactory.getLog(FetcherRunnable.class);
    
    public FetcherRunnable(String broker,
                           List<PartitionTopicInfo> partitionTopicInfos) {
        this.broker = broker;
        this.partitionTopicInfos = partitionTopicInfos;
        this.simpleConsumer = new SimpleConsumer(Util.getHostFromBroker(broker), Util.getPortFromBroker(broker));
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
            buf.append(format("%s-%s-%s,", pti.topic, pti.brokerString, pti.partition));
        }
        buf.append(']');
        logger.info(String.format("%s comsume at %s:%d with %s", getName(), Util.getHostFromBroker(broker), Util.getPortFromBroker(broker), buf.toString()));
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
            fetches.add(new FetchRequest(info.topic, info.partition, info.getFetchedOffset(), Consumer.config.getFetchSize()));
        }
        MultiFetchResponse response = simpleConsumer.multifetch(fetches);
        int index = 0;
        long read = 0L;
        for (ByteBufferMessageSet messages : response) {
            PartitionTopicInfo info = partitionTopicInfos.get(index);
            try {
                read += processMessages(messages, info);
            } catch (IOException e) {
                throw e;
            } catch (InterruptedException e) {
                if (!stopped) {
                    logger.error("error in FetcherRunnable for " + info, e);
                }
                throw e;
            } catch (RuntimeException e) {
                if (!stopped) {
                    logger.error("error in FetcherRunnable for " + info, e);
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
        if (messages.isOffsetOutOfRange()) {
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

    private long resetConsumerOffsets(String topic, String partition) throws IOException {
        long offset = -1;
        String autoOffsetReset = Consumer.config.getAutoOffsetReset();
        if (OffsetRequest.SMALLES_TIME_STRING.equals(autoOffsetReset)) {
            offset = OffsetRequest.EARLIES_TTIME;
        } else if (OffsetRequest.LARGEST_TIME_STRING.equals(autoOffsetReset)) {
            offset = OffsetRequest.LATES_TTIME;
        }
        //
        long newOffset = simpleConsumer.getOffsetsBefore(topic, partition, offset, 1);
        ConsumerConnector connector = ConsumerConnector.getInstance();
        connector.updateOffset(topic, partition, newOffset);
        return newOffset;
    }
}

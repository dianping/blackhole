package com.dp.blackhole.consumer;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.collectornode.persistent.MessageAndOffset;
import com.dp.blackhole.consumer.exception.ConsumerTimeoutException;

import static java.lang.String.format;

public class ConsumerIterator<T> extends IteratorTemplate<T> {
    
    private final Log logger = LogFactory.getLog(ConsumerIterator.class);
    
    final String topic;

    final BlockingQueue<FetchedDataChunk> queue;

    final int consumerTimeoutMs;

    final Decoder<T> decoder;

    private AtomicReference<Iterator<MessageAndOffset>> current = new AtomicReference<Iterator<MessageAndOffset>>(null);

    private PartitionTopicInfo currentTopicInfo = null;

    private long consumedOffset = -1L;

    public ConsumerIterator(String topic, BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs,
                            Decoder<T> decoder) {
        super();
        this.topic = topic;
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.decoder = decoder;
    }

    @Override
    public T next() {
        T decodedMessage = super.next();
        if (consumedOffset < 0) {
            throw new IllegalStateException("Offset returned by the message set is invalid " + consumedOffset);
        }
        currentTopicInfo.resetConsumeOffset(consumedOffset);
        return decodedMessage;
    }

    @Override
    protected T makeNext() {
        try {
            return makeNext0();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected T makeNext0() throws InterruptedException {
        FetchedDataChunk currentDataChunk = null;
        Iterator<MessageAndOffset> localCurrent = current.get();
        if (localCurrent == null || !localCurrent.hasNext()) {
            if (consumerTimeoutMs < 0) {
                currentDataChunk = queue.take();
            } else {
                currentDataChunk = queue.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS);
                if (currentDataChunk == null) {
                    resetState();
                    throw new ConsumerTimeoutException("consumer timeout in " + consumerTimeoutMs + " ms");
                }
            }
            if (currentDataChunk == ConsumerConnector.SHUTDOWN_COMMAND) {
                logger.warn("Now closing the message stream");
                queue.offer(currentDataChunk);
                return allDone();
            } else {
                currentTopicInfo = currentDataChunk.topicInfo;
                if (currentTopicInfo.getConsumedOffset() < currentDataChunk.fetchOffset) {
                    logger.error(format(
                            "consumed offset: %d doesn't match fetch offset: %d for %s;\n Consumer may lose data", //
                            currentTopicInfo.getConsumedOffset(), currentDataChunk.fetchOffset, currentTopicInfo));
                    currentTopicInfo.resetConsumeOffset(currentDataChunk.fetchOffset);
                }
                localCurrent = currentDataChunk.messages.getItertor();
                current.set(localCurrent);
            }
        }
        MessageAndOffset item = localCurrent.next();
        while (item.offset < currentTopicInfo.getConsumedOffset() && localCurrent.hasNext()) {
            item = localCurrent.next();
        }
        consumedOffset = item.offset;
        return decoder.toEvent(item.message);
    }

    public void clearCurrentChunk() {
        if (current.get() != null) {
            logger.info("Clearing the current data chunk for this consumer iterator");
            current.set(null);
        }
    }
}

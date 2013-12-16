package com.dp.blackhole.consumer;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.collectornode.persistent.MessageAndOffset;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.consumer.exception.ConsumerTimeoutException;

public class ConsumerIterator implements Iterator<String> {
    
    private final Log logger = LogFactory.getLog(ConsumerIterator.class);
    
    final String topic;

    final BlockingQueue<FetchedDataChunk> queue;

    final int consumerTimeoutMs;
    
    enum State {
        DONE, READY, NOT_READY, FAILED;
    }

    private State state = State.NOT_READY;
    
    private String nextItem = null;

    private Iterator<MessageAndOffset> current = null;

    private PartitionTopicInfo currentTopicInfo = null;

    private long consumedOffset = -1L;

    public ConsumerIterator(String topic, BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs) {
        super();
        this.topic = topic;
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
    }

    @Override
    public String next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.NOT_READY;
        String message = nextItem;
        if (consumedOffset < 0) {
            throw new IllegalStateException("Offset returned by the message set is invalid " + consumedOffset);
        }
        currentTopicInfo.resetConsumeOffset(consumedOffset);
        return message;
    }

    protected String makeNext() throws InterruptedException, ConsumerTimeoutException {
        FetchedDataChunk currentDataChunk = null;
        if (current == null || !current.hasNext()) {
            if (consumerTimeoutMs < 0) {
                currentDataChunk = queue.take();
            } else {
                currentDataChunk = queue.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS);
                if (currentDataChunk == null) {
                    state = State.NOT_READY;
                    throw new ConsumerTimeoutException("consumer timeout in " + consumerTimeoutMs + " ms");
                }
            }
            if (currentDataChunk == Consumer.SHUTDOWN_COMMAND) {
                logger.warn("Now closing the message stream");
                state = State.DONE;
                return null;
            } else {
                currentTopicInfo = currentDataChunk.topicInfo;
                if (currentTopicInfo.getConsumedOffset() > currentDataChunk.fetchOffset) {
                    logger.error("consumed offset: " + currentTopicInfo.getConsumedOffset() 
                            + " doesn't match fetch offset: " 
                            + currentDataChunk.fetchOffset + " for " 
                            + currentTopicInfo + ";\n Consumer may lose data");
                    currentTopicInfo.resetConsumeOffset(currentDataChunk.fetchOffset);
                }
                current = currentDataChunk.messages.getItertor();
            }
        }
        MessageAndOffset item = current.next();
        while (item.offset < currentTopicInfo.getConsumedOffset() && current.hasNext()) {
            item = current.next();
        }
        consumedOffset = item.offset;
//        logger.debug(item.message + "  @  " +item.offset);
        return Util.toEvent(item.message);
    }

    public void clearCurrentChunk() {
        current = null;
        logger.info("Clearing the current data chunk for this consumer iterator");
    }

    @Override
    public boolean hasNext() {
        switch (state) {
        case FAILED:
            throw new IllegalStateException("Iterator is in failed state");
        case DONE:
            return false;
        case READY:
            return true;
        case NOT_READY:
            break;
        }
        state = State.FAILED;
        try {
            nextItem = makeNext();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (ConsumerTimeoutException e) {
            state = State.DONE;
        }
        if (state == State.DONE) return false;
        state = State.READY;
        return true;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

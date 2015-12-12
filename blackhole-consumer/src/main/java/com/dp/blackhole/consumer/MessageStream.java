package com.dp.blackhole.consumer;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import com.dp.blackhole.consumer.api.MessagePack;

public class MessageStream implements Iterable<MessagePack> {

    public final String topic;

    public final int consumerTimeoutMs;

    private final ConsumerIterator consumerIterator;
    
    private AtomicReference<Thread> userWorkThread;

    public MessageStream(String topic, BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs) {
        super();
        this.topic = topic;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.consumerIterator = new ConsumerIterator(topic, queue, consumerTimeoutMs);
    }

    public Thread getUserWorkThread() {
        return userWorkThread.get();
    }

    @Override
    public Iterator<MessagePack> iterator() {
        this.userWorkThread = new AtomicReference<Thread>(Thread.currentThread());
        return consumerIterator;
    }

    /**
     * This method clears the queue being iterated during the consumer
     * rebalancing. This is mainly to reduce the number of duplicates
     * received by the consumer
     */
    public void clear() {
        consumerIterator.clearCurrentChunk();
    }

}

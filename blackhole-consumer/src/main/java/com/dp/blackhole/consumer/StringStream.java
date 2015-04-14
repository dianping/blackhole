package com.dp.blackhole.consumer;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

import com.dp.blackhole.consumer.ConsumerIterator.StringIterator;

public class StringStream implements Iterable<String> {

    public final String topic;

    public final int consumerTimeoutMs;
    
    private ConsumerIterator consumerIterator;

    private final StringIterator stringIterator;

    public StringStream(String topic, BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs) {
        super();
        this.topic = topic;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.consumerIterator = new ConsumerIterator(topic, queue, consumerTimeoutMs);
        this.stringIterator = consumerIterator.new StringIterator();
    }

    @Override
    public Iterator<String> iterator() {
        return stringIterator;
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

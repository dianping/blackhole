package com.dp.blackhole.consumer;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

public class MessageStream<T> implements Iterable<T> {

    final String topic;

    final BlockingQueue<FetchedDataChunk> queue;

    final int consumerTimeoutMs;

    final Decoder<T> decoder;

    private final ConsumerIterator<T> consumerIterator;

    public MessageStream(String topic, BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs, Decoder<T> decoder) {
        super();
        this.topic = topic;
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.decoder = decoder;
        this.consumerIterator = new ConsumerIterator<T>(topic, queue, consumerTimeoutMs, decoder);
    }

    @Override
    public Iterator<T> iterator() {
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

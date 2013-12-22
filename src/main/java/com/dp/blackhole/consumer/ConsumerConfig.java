package com.dp.blackhole.consumer;

import java.util.Properties;

import com.dp.blackhole.collectornode.persistent.protocol.OffsetRequest;

public class ConsumerConfig {

    private int fetchSize;

    private int maxQueuedChunks;

    private String autoOffsetReset;

    private int consumerTimeoutMs;
    
    private boolean multiFetch;

    public ConsumerConfig(Properties props) {
        this.fetchSize = getInt(props, "fetch.size", 1024 * 1024);//1MB
        this.maxQueuedChunks = getInt(props, "queuedchunks.max", 2);
        this.autoOffsetReset = getString(props, "autooffset.reset", OffsetRequest.SMALLES_TIME_STRING);
        this.multiFetch = getBoolean(props, "messages.multiFetch", false);
        this.consumerTimeoutMs = getInt(props, "consumer.timeout.ms", -1);
    }

    public ConsumerConfig() {
        this(new Properties());
    }
    
    /** the number of byes of messages to attempt to fetch */
    public int getFetchSize() {
        return fetchSize;
    }

    /** max number of messages buffered for consumption */
    public int getMaxQueuedChunks() {
        return maxQueuedChunks;
    }

    /**
     * what to do if an offset is out of range.
     * 
     * <pre>
     *     smallest : automatically reset the offset to the smallest offset
     *     largest : automatically reset the offset to the largest offset
     *     anything else: throw exception to the consumer
     * </pre>
     */
    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    /**
     * throw a timeout exception to the consumer if no message is available for consumption
     * after the specified interval
     */
    public int getConsumerTimeoutMs() {
        return consumerTimeoutMs;
    }

    public boolean isMultiFetch() {
        return multiFetch;
    }
    
    public boolean getBoolean(Properties props, String name, boolean defaultValue) {
        if (!props.containsKey(name)) return defaultValue;
        return "true".equalsIgnoreCase(props.getProperty(name));
    }
    
    public int getInt(Properties props, String name, int defaultValue) {
        int v = defaultValue;
        if (props.containsKey(name)) {
            v = Integer.valueOf(props.getProperty(name));
        }
        return v;
    }
    
    public String getString(Properties props, String name, String defaultValue) {
        return props.getProperty(name, defaultValue);
    }
}

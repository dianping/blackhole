package com.dp.blackhole.consumer;

import java.util.Properties;

import com.dp.blackhole.collectornode.persistent.protocol.OffsetRequest;

public class ConsumerConfig {
    
    private String supervisorHost;
    
    private int supervisorPort;

    private int fetchSize;

    private boolean autoCommit;

    private int autoCommitIntervalMs;

    private int maxQueuedChunks;

    private String autoOffsetReset;

    private int consumerTimeoutMs;
    
    private boolean betterOrdered;

    public ConsumerConfig(Properties props) {
        this.supervisorHost = getString(props, "supervisor.host", "localhost");
        this.supervisorPort = getInt(props, "supervisor.port", 8080);
        this.fetchSize = getInt(props, "fetch.size", 1024 * 1024);//1MB
        this.autoCommit = getBoolean(props, "autocommit.enable", true);
        this.autoCommitIntervalMs = getInt(props, "autocommit.interval.ms", 1000);//1 seconds
        this.maxQueuedChunks = getInt(props, "queuedchunks.max", 10);
        this.autoOffsetReset = getString(props, "autooffset.reset", OffsetRequest.SMALLES_TIME_STRING);
        this.betterOrdered = getBoolean(props, "messages.betterOrdered", false);
    }

    public String getSupervisorHost() {
        return supervisorHost;
    }

    public void setSupervisorHost(String supervisorHost) {
        this.supervisorHost = supervisorHost;
    }

    public int getSupervisorPort() {
        return supervisorPort;
    }

    public void setSupervisorPort(int supervisorPort) {
        this.supervisorPort = supervisorPort;
    }

    /** the number of byes of messages to attempt to fetch */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * if true, periodically commit to zookeeper the offset of messages already fetched by the
     * consumer
     */
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * the frequency in ms that the consumer offsets are committed to zookeeper
     */
    public int getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
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

    public boolean isBetterOrdered() {
        return betterOrdered;
    }

    /**
     * @param betterOrdered if true, the latency of message receiving will be increase,
     * but messages receiving form different partitions are synchronous and better time ordered.
     */
    public void setBetterOrdered(boolean betterOrdered) {
        this.betterOrdered = betterOrdered;
    }
}

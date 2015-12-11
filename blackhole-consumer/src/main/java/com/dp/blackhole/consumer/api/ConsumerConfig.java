package com.dp.blackhole.consumer.api;

import java.util.Properties;

import com.dp.blackhole.protocol.data.OffsetRequest;

public class ConsumerConfig {

    private int fetchSize;

    private int maxQueuedChunks;

    private String autoOffsetReset;

    private int consumerTimeoutMs;
    
    private boolean multiFetch;
    
    private String supervisorHost;
    
    private int supervisorPort;

    /**
     * the properties entry include:<br/>
     * <b>fetch.size</b> the max number of byes of messages to attempt to fetch, default is 1MB<br/>
     * <b>queuedchunks.max</b> the max number of messages buffered for consumption, default is 2<br/>
     * <b>autooffset.reset</b> what to do if an offset is out of range, default is reseted to smallest<br/>
     * <b>messages.multiFetch</b> whether or not fetch multiple partition message at once, default is false<br/>
     * <b>consumer.timeout.ms</b> timeout of message fetching, default is -1, standing for waiting until an message becomes available<br/>
     * all configuration is set to default.
     * if you want configure by yourself, use setXX() to overwrite
     */
    public ConsumerConfig() {
        this.fetchSize = 1024 * 1024;//1MB
        this.maxQueuedChunks = 2;
        this.autoOffsetReset = OffsetRequest.SMALLES_TIME_STRING;
        this.multiFetch = false;
        this.consumerTimeoutMs = -1;
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

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public void setMaxQueuedChunks(int maxQueuedChunks) {
        this.maxQueuedChunks = maxQueuedChunks;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    public void setConsumerTimeoutMs(int consumerTimeoutMs) {
        this.consumerTimeoutMs = consumerTimeoutMs;
    }

    public void setMultiFetch(boolean multiFetch) {
        this.multiFetch = multiFetch;
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
            try {
                v = Integer.valueOf(props.getProperty(name));
            } catch (Exception e) {
            }
        }
        return v;
    }
    
    public String getString(Properties props, String name, String defaultValue) {
        return props.getProperty(name, defaultValue);
    }
}

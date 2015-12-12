package com.dp.blackhole.consumer.api;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.consumer.ConsumerConnector;
import com.dp.blackhole.consumer.FetchedDataChunk;
import com.dp.blackhole.consumer.MessageStream;
import com.dp.blackhole.consumer.exception.ConsumerInitializeException;

public class Consumer {
    private final Log LOG = LogFactory.getLog(Consumer.class);
    public static final FetchedDataChunk SHUTDOWN_COMMAND = new FetchedDataChunk(null, null, -1);
    
    private LinkedBlockingQueue<FetchedDataChunk> queue;
    
    private String topic;
    private String group;
    private String consumerId;
    private ConsumerConfig config;
    private ConsumerConnector connector;
    private OffsetStrategy offsetStrategy;
    
    private ConcurrentHashMap<String, MessageStream> currentStreams;
    
    public Consumer(String topic, String group, ConsumerConfig config) throws ConsumerInitializeException {
        this(topic, group, config, new TailOffsetStrategy());
    }
    
    public Consumer(String topic, String group, ConsumerConfig config, OffsetStrategy offsetStrategy) throws ConsumerInitializeException {
        this.topic = topic;
        this.group = group;
        this.config = config;
        this.offsetStrategy = offsetStrategy;
        consumerId = generateConsumerId(this.group);
        queue = new LinkedBlockingQueue<FetchedDataChunk>(this.config.getMaxQueuedChunks());
        currentStreams = new ConcurrentHashMap<String, MessageStream>();
        connector = ConsumerConnector.getInstance();
        try {
            if (!connector.initialized) {
                connector.init(config);
            }
        } catch (Exception e) {
            LOG.error("Initialize consumer fail.", e);
            throw new ConsumerInitializeException("Initialize consumer fail.", e);
        }
    }
    
    public void start() {
        connector.registerConsumer(topic, group, consumerId ,this);
    }
    
    public LinkedBlockingQueue<FetchedDataChunk> getDataQueue() {
        return queue;
    }
    
    public void clearQueue() {
        queue.clear();
    }
    
    public void shutdown() throws InterruptedException {
        queue.put(SHUTDOWN_COMMAND);
    }
    
    public String getTopic() {
        return topic;
    }

    public String getGroup() {
        return group;
    }
    
    public OffsetStrategy getOffsetStrategy() {
        return offsetStrategy;
    }

    public String getConsumerId() {
        return consumerId;
    }
    
    public ConsumerConfig getConf() {
        return config;
    }
    
    public MessageStream getStream() {
        MessageStream stream = new MessageStream(topic, queue, config.getConsumerTimeoutMs());
        currentStreams.putIfAbsent(topic, stream);
        return currentStreams.get(topic);
    }

    /**
     * generate random consumerid ( hostname-currenttime-uuid.sub(8) )
     *
     * @return random consumerid
     */
    private String generateConsumerId(String group) {
        UUID uuid = UUID.randomUUID();
        try {
            return group + "-" + InetAddress.getLocalHost().getHostName() + "-"
                    + System.currentTimeMillis() + "-"
                    + Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(
                    "can not generate consume id by auto, set the 'consumerid' parameter to fix this");
        }
    }
    
    public Thread getUserWorkThread() {
        MessageStream stream = currentStreams.get(topic);
        if (stream == null) {
            return null;
        } else {
            return stream.getUserWorkThread();
        }
    }
}
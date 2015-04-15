package com.dp.blackhole.consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;
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
    private OffsetReferee offsetReferee;

    public Consumer(String topic, String group, ConsumerConfig config) throws ConsumerInitializeException {
        this.topic = topic;
        this.group = group;
        this.config = config;
        consumerId = generateConsumerId(this.group);
        queue = new LinkedBlockingQueue<FetchedDataChunk>(this.config.getMaxQueuedChunks());
        
        connector = ConsumerConnector.getInstance();
        try {
            Class<?> offsetRefereeClazz = Class.forName(config.getOffsetRefereeClassName());
            this.offsetReferee = (OffsetReferee) Util.newInstance(offsetRefereeClazz);
            if (!connector.initialized) {
                connector.init();
            }
        } catch (Exception e) {
            LOG.error("Initialize consumer fail.", e);
            throw new ConsumerInitializeException("Initialize consumer fail.", e);
        }
    }
    
    public void start() {
        connector.registerConsumer(topic, group, consumerId ,this);
    }
    
    LinkedBlockingQueue<FetchedDataChunk> getDataQueue() {
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
    
    public OffsetReferee getOffsetReferee() {
        return offsetReferee;
    }

    public String getConsumerId() {
        return consumerId;
    }
    
    public ConsumerConfig getConf() {
        return config;
    }

    public MessageStream getMessageStream() {
        return new MessageStream(topic, queue, config.getConsumerTimeoutMs());
    }
    
    public StringStream getStringStream() {
        return new StringStream(topic, queue, config.getConsumerTimeoutMs());
    }

    @Deprecated
    public StringStream getStream() {
        return getStringStream();
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
}
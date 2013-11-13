package com.dp.blackhole.consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Consumer {
    
    private static final Log LOG = LogFactory.getLog(ConsumerConnector.class);
    
    private static boolean running = false;
    
    static ConsumerConfig config;
    
    public synchronized static void initEnv(Properties prop) {
        config = new ConsumerConfig(prop);
        ConsumerConnector connector = ConsumerConnector.getInstance();
        if (!running) {
            running = true;
            Thread thread = new Thread(connector);
            thread.setDaemon(true);
            thread.start();
        } else {
            LOG.info("Connector thread has already started");
        }
    }
    
    public static String createConsumer (final String topic, final String group, 
            final int minConsumersInGroup) {
        String consumerUuid = generateConsumerId();
        LOG.info("create message stream by consumerid " + consumerUuid + " with groupid " + group);
        //consumerIdString => groupid_consumerid
        final String consumerIdString = group + "_" + consumerUuid;
        ConsumerConnector connector = ConsumerConnector.getInstance();
        connector.registerConsumer(consumerIdString, topic, minConsumersInGroup);
        return consumerIdString;
    }
    
    public static MessageStream getStreamByConsumer(String topic, String consumerIdString) {
        return getStreamByConsumer(topic, consumerIdString, -1);
    }
    
    /**
     * consumerTimeoutMs: throw a timeout exception to the consumer 
     * if no message is available for consumption after the specified interval
     * -1 never time out
     */
    public synchronized static MessageStream getStreamByConsumer(String topic, String consumerIdString,
            int consumerTimeoutMs) {
        ConsumerConnector connector = ConsumerConnector.getInstance();
        return new MessageStream(topic, connector.queues.get(consumerIdString), consumerTimeoutMs);
    }

    /**
     * generate random consumerid ( hostname-currenttime-uuid.sub(8) )
     *
     * @return random consumerid
     */
    private static String generateConsumerId() {
        UUID uuid = UUID.randomUUID();
        try {
            return InetAddress.getLocalHost().getHostName() + "-"
                    + System.currentTimeMillis() + "-"
                    + Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(
                    "can not generate consume id by auto, set the 'consumerid' parameter to fix this");
        }
    }
    
    public static void main(String[] args) {
        String topic = "topic1";
        String group = "g1";
        Properties properties = new Properties();
        properties.setProperty("bla", "bla");
        Consumer.initEnv(properties);
        String consumerIdString = Consumer.createConsumer(topic, group, 3);
        MessageStream stream = Consumer.getStreamByConsumer(topic, consumerIdString, 5000);
        for (String message : stream) {
            System.out.println(message);
        }
    }
}
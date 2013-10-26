package com.dp.blackhole.consumer;

import static java.lang.String.format;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Consumer {
    
    private static final Log LOG = LogFactory.getLog(ConsumerConnector.class);
    
    private static boolean running = false;
    
    public synchronized static void initEnv() {
        ConsumerConnector connector = ConsumerConnector.getInstance();
        if (!running) {
            running = true;
            Thread thread = new Thread(connector);
            thread.start();
        } else {
            LOG.info("Connector thread has already started");
        }
    }
    
    public static String createConsumer (final String topic, final String group, 
            final int minConsumersInGroup) {
        String consumerUuid = generateConsumerId();
        LOG.info(format("create message stream by consumerid [%s] with groupid [%s]", consumerUuid,
                group));
        //consumerIdString => groupid_consumerid
        final String consumerIdString = group + "_" + consumerUuid;
        ConsumerConnector connector = ConsumerConnector.getInstance();
        connector.registerConsumer(consumerIdString, topic, minConsumersInGroup);
        return consumerIdString;
    }
    
    public static <T> MessageStream<T> getStreamByConsumer(String topic, String consumerIdString,
            int consumerTimeoutMs, Decoder<T> decoder) {
        ConsumerConnector connector = ConsumerConnector.getInstance();
        return new MessageStream<T>(topic, connector.queues.get(consumerIdString), consumerTimeoutMs, decoder);
    }

    /**
     * generate random consumerid ( hostname-currenttime-uuid.sub(8) )
     *
     * @return random consumerid
     */
    private static String generateConsumerId() {
        UUID uuid = UUID.randomUUID();
        try {
            return format("%s-%d-%s", InetAddress.getLocalHost().getHostName(), //
                    System.currentTimeMillis(),//
                    Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(
                    "can not generate consume id by auto, set the 'consumerid' parameter to fix this");
        }
    }
}

package com.dp.blackhole.consumer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import com.dp.blackhole.common.Util;

public class Consumer {
    public static final FetchedDataChunk SHUTDOWN_COMMAND = new FetchedDataChunk(null, null, -1);
    
    private LinkedBlockingQueue<FetchedDataChunk> queue;
    
    private String topic;
    private String group;
    private String consumerId;
    private int ConsumerTimeoutMs;
    
    public Consumer(String topic, String group) {
        this(topic, group, -1, 2);
    }
    
    public Consumer(String topic, String group, int ConsumerTimeoutMs) {
        this(topic, group, ConsumerTimeoutMs, 2);
    }
    
    public Consumer(String topic, String group, int ConsumerTimeoutMs, int dataQueueSize) {
        this.topic = topic;
        this.group = group;
        consumerId = generateConsumerId(group);
        queue = new LinkedBlockingQueue<FetchedDataChunk>(dataQueueSize);
        ConsumerConnector.getInstance().registerConsumer(topic, group, consumerId ,this);
        this.ConsumerTimeoutMs = ConsumerTimeoutMs;
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
    
    public MessageStream getStream() {
        return new MessageStream(topic, queue, ConsumerTimeoutMs);
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
    
    public static void main(String[] args) {
        String topic = "openAPI1";
        String group = "testgroup";
        Properties properties = new Properties();

        ConsumerConfig config = new ConsumerConfig(properties);
        ConsumerConnector.init(config);
        Consumer consumer = new Consumer(topic, group);
        MessageStream stream = consumer.getStream();
        
        try {
            RandomAccessFile r = new RandomAccessFile("/tmp/r.txt", "rw");
            r.seek(r.length());
            
            for (String message : stream) {
                r.writeBytes(message + " " +Util.getTS() +"\n");

            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
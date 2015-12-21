package com.dp.blackhole.producer;

import java.util.Properties;

import com.dp.blackhole.common.Util;

public class TestSimpleProducer {
    
    public static void main(String[] args) throws InterruptedException {
        String supervisorHost = args[0];
        String supervisorPort = args[1];
        String topic = args[2];
        Properties properties = new Properties();
        properties.setProperty("supervisor.host", supervisorHost);
        properties.setProperty("supervisor.port", supervisorPort);
        properties.setProperty("producer.linger.ms", "3000");
        Producer producer = new Producer(topic, properties);
        producer.register();
        while (true) {
            send(producer);
        }
    }
    
    private static void send(Producer producer) {
        long delay = 1000L;
        String message = Util.getTS() + "-------------";
        boolean ret1 = producer.sendMessage(message);
        if (ret1) {
            System.out.println("sended " + Util.getTS());
        } else {
            System.out.println("not send cause ioexcption");
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
        }
    }
}

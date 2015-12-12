package com.dp.blackhole.producer;

import java.util.Properties;

import com.dp.blackhole.common.Util;

public class TestSimpleProducer {
    
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("supervisor.host", "localhost");
        properties.setProperty("supervisor.port", "8080");
        properties.setProperty("producer.linger.ms", "3000");
        Producer producer = new Producer("testproducer", properties);
        producer.register();
        while (true) {
            send(producer);
        }
    }
    
    private static void send(Producer producer) {
        long delay = 10000L;
        String message = Util.getTS() + "-------------";
        boolean ret1 = producer.sendMessage(message);
        System.out.println("sended " + Util.getTS());
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
        }
    }
}

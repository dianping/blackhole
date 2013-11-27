package com.dp.blackhole.collectornode;

import java.io.IOException;
import java.util.Properties;

import com.dp.blackhole.common.Util;

public class testPublisherExecutor {
    public static void main(String[] args) throws IOException {
        
        Properties prop = new Properties();
        prop.setProperty("publisher.storage.dir", "/tmp/base");
        Publisher p = new Publisher(prop);
        
        Producer producer = new Producer();
        producer.setPublisher(p.getExecutor());
        
        while (true) {
            String str = Long.toString(Util.getTS());
            producer.send("test", "localhost-1", str.getBytes());
        }
        
    }
}

package com.dp.blackhole.collectornode;

import java.io.IOException;

import com.dp.blackhole.collectornode.persistent.PersistentManager;
import com.dp.blackhole.collectornode.persistent.Producer;
import com.dp.blackhole.common.Util;

public class testPublisher {
    public static void main(String[] args) throws IOException {
        PersistentManager mananger = new PersistentManager("/tmp/base", 1024*1024*50, 1024*1024);
        
        Publisher publisher = new Publisher(mananger);
        
        Producer producer = new Producer();
        producer.setPublisher(publisher);
        
        while (true) {
            String str = Long.toString(Util.getTS());
            producer.send("test", "localhost-1", str.getBytes());
        }
        
    }
}

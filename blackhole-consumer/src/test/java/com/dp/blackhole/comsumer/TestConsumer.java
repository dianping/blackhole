package com.dp.blackhole.comsumer;

import com.dianping.lion.client.LionException;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.consumer.Consumer;
import com.dp.blackhole.consumer.ConsumerConfig;
import com.dp.blackhole.consumer.ConsumerConnector;
import com.dp.blackhole.consumer.MessageStream;

public class TestConsumer {
    
    public static void main( String[] args ) throws LionException, InterruptedException {
        
        args = new String[] {"testblackhole2","test321", "1000"};
        
        String topic = args[0];
        String group = args[1];
        int num = Integer.parseInt(args[2]);

        ConsumerConnector.getInstance().init("192.168.213.245", 8081, true, 3000000);
        ConsumerConfig config = new ConsumerConfig();
        Consumer consumer = new Consumer(topic, group, config);
        consumer.start();
        MessageStream stream = consumer.getStream();
        
        long start = Util.getTS();
        int i =0;
        
        for (String message : stream) {
//            if (i == num) {
//                break;
//            }
            System.out.println(message);
            i++;
        }
        ConsumerConnector.shutdownNow();
        long end = Util.getTS();
        double k = 1000.0;
        double time = (end -start)/k;
        System.out.println("run time: " + time);
        System.out.println("throughout: " + i/time);
    }
}

package com.dp.blackhole.consumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.lion.client.LionException;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.consumer.Consumer;
import com.dp.blackhole.consumer.ConsumerConfig;
import com.dp.blackhole.consumer.ConsumerConnector;
import com.dp.blackhole.consumer.MessageStream;

public class TestConsumer {
   
    private static final Log LOG = LogFactory.getLog(TestConsumer.class); 
    public static void main( String[] args ) throws LionException, InterruptedException {
        
//        args = new String[] {"localtest","t123", "10000000"};
        
        String supervisorHost = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        String group = args[3];
        int num = Integer.parseInt(args[4]);

        ConsumerConnector.getInstance().init(supervisorHost, port, true, 6000);
        ConsumerConfig config = new ConsumerConfig();
        Consumer firstConsumerInOneProcess = new Consumer(topic, group, config);
        firstConsumerInOneProcess.start();
        MessageStream firstStream = firstConsumerInOneProcess.getStream();
        
        Consumer secondConsumerInOneProcess = new Consumer(topic, group, config);
        secondConsumerInOneProcess.start();
        MessageStream secondStream = secondConsumerInOneProcess.getStream();
        
        long start = Util.getTS();
        
        Thread t1 = new MessageConsumeThread(firstStream, num);
        t1.start();
        Thread t2 = new MessageConsumeThread(secondStream, num);
        t2.start();
        
        t1.join();
        t2.join();
        ConsumerConnector.shutdownNow();
        long end = Util.getTS();
        double k = 1000.0;
        double time = (end -start)/k;
        System.out.println("run time: " + time);
//        System.out.println("throughout: " + i/time);
    }
    
    static class MessageConsumeThread extends Thread {
        private MessageStream stream;
        private int num;
        public MessageConsumeThread(MessageStream stream, int num) {
            this.stream = stream;
            this.num = num;
        }
        @Override
        public void run() {
            int i =0;
            for (String message : stream) {
                if (i % num == 0) {
                    LOG.info("consumed: " + i);
                }
//                System.out.println(message);
                i++;
            }
        }
    }
}

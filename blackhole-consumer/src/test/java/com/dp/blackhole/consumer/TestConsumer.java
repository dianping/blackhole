package com.dp.blackhole.consumer;
import java.util.ArrayList;
import java.util.List;

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
        int numConsumerInOneProcess = Integer.parseInt(args[4]);
        if (numConsumerInOneProcess < 1 || numConsumerInOneProcess > 10) {
            throw new RuntimeException("numConsumerInOneProcess must greater than 0 and less than 10");
        }
        List<MessageStream> messageStreams = new ArrayList<MessageStream>();

        ConsumerConnector.getInstance().init(supervisorHost, port, true, 6000);
        ConsumerConfig config = new ConsumerConfig();
        
        for (int i = 0; i < numConsumerInOneProcess; i++) {
            Consumer consumer = new Consumer(topic, group, config);
            consumer.start();
            MessageStream stream = consumer.getStream();
            messageStreams.add(stream);
        }
        long start = Util.getTS();
        for (MessageStream stream : messageStreams) {
            Thread t = new MessageConsumeThread(stream);
            t.start();
        }
        
        while (true) {
            Thread.sleep(1000);
        }
    }
    
    static class MessageConsumeThread extends Thread {
        private MessageStream stream;
        public MessageConsumeThread(MessageStream stream) {
            this.stream = stream;
        }
        @Override
        public void run() {
            int i =0;
            for (String message : stream) {
                if (i % 100000 == 0) {
                    LOG.info("consumed: " + i);
                }
//                System.out.println(message);
                i++;
            }
        }
    }
}

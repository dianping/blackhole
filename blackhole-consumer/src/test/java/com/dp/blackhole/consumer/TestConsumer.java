package com.dp.blackhole.consumer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.consumer.ConsumerConnector;
import com.dp.blackhole.consumer.api.CommittedOffsetStrategy;
import com.dp.blackhole.consumer.api.Consumer;
import com.dp.blackhole.consumer.api.ConsumerConfig;
import com.dp.blackhole.consumer.api.MessagePack;
import com.dp.blackhole.consumer.api.TailOffsetStrategy;
import com.dp.blackhole.consumer.api.decoder.StringDecoder;

public class TestConsumer {
   
    private static final Log LOG = LogFactory.getLog(TestConsumer.class);
    public static void main( String[] args ) throws InterruptedException {
        
//        args = new String[] {"localtest","t123", "10000000"};
        
        List<MessageConsumeThread> runningConsumeThread = new ArrayList<MessageConsumeThread>();
        String debugFlag = args[0];
        boolean debug = false;
        int debugIndex = 0;
        if (debugFlag.equalsIgnoreCase("--debug")) {
            debug = true;
            debugIndex = 1;
        }
        
        String supervisorHost = args[0 + debugIndex];
        int port = Integer.parseInt(args[1 + debugIndex]);
        String topic = args[2 + debugIndex];
        String group = args[3 + debugIndex];
        int numConsumerInOneProcess = Integer.parseInt(args[4 + debugIndex]);
        String fromTail = args[5 + debugIndex];
        long durationMills = Long.MAX_VALUE;
        try {
            durationMills = Long.parseLong(args[6 + debugIndex]);
        } catch (Exception e) {
        }

        if (numConsumerInOneProcess < 1 || numConsumerInOneProcess > 10) {
            throw new RuntimeException("numConsumerInOneProcess must greater than 0 and less than 10");
        }
        List<MessageStream> messageStreams = new ArrayList<MessageStream>();

        ConsumerConnector.getInstance().init(supervisorHost, port, true, 6000);
        Properties prop = new Properties();
        prop.put("consumer.offsetRefereeClass.name", TailOffsetStrategy.class.getCanonicalName());
        prop.put("consumer.isSubscribeFromTail", fromTail);
        ConsumerConfig config = new ConsumerConfig(prop);
        
        for (int i = 0; i < numConsumerInOneProcess; i++) {
            Consumer consumer = new Consumer(topic, group, config, new CommittedOffsetStrategy());
            consumer.start();
            MessageStream stream = consumer.getStream();
            messageStreams.add(stream);
        }
        long start = Util.getTS();
        for (MessageStream stream : messageStreams) {
            MessageConsumeThread t = new MessageConsumeThread(stream, debug);
            t.start();
            runningConsumeThread.add(t);
        }
        long timeCount = 0L;
        while (true) {
            Thread.sleep(1000);
            timeCount += 1000L;
            if (timeCount > durationMills) {
                for(MessageConsumeThread c : runningConsumeThread) {
                    c.shutdown();
                }
                break;
            }
        }
        long end = Util.getTS();
        LOG.info("TestConsumer stop, duration is " + (end - start) + " mills seconds");
    }
    
    static class MessageConsumeThread extends Thread {
        private MessageStream stream;
        private volatile boolean running;
        private boolean debug;
        private StringDecoder decoder;
        public MessageConsumeThread(MessageStream stream, boolean debug) {
            this.stream = stream;
            this.running = true;
            this.debug = debug;
            this.decoder = new StringDecoder();
        }
        
        @Override
        public void run() {
            LOG.info("WORK THREAD " + Thread.currentThread().getName());
            while (running) {
                long i =0;
                for (MessagePack entity : stream) {
                    if (i % 100000L == 0) {
                        LOG.info("consumed: " + i);
                    }
                    if (debug) {
                        System.out.println(decoder.decode(entity));
                    }
                    i++;
                }
            }
        }
        
        public void shutdown() {
            running = false;
        }
    }
}

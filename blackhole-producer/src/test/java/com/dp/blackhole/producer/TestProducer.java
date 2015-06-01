package com.dp.blackhole.producer;

public class TestProducer {
    
    public static void main(String[] args) {
        Producer producer1 = new Producer(args[0]);
        Producer producer2 = new Producer(args[0]);
        producer1.register();
        producer2.register();
        for (int i = 0; i < 1000; i++) {
            long delay = 10L;
            String message = i + "----------------------------------";
            boolean ret1 = producer1.sendMessage(message);
            boolean ret2 = producer2.sendMessage(message);
            if (!(ret1 && ret2)) {
                delay = 1000;
            }
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
            }
        }
        producer1.closeProducer();
        producer2.closeProducer();
    }
}

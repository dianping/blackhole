package com.dp.blackhole.scaleout;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkQueue {
    private static final int numThreads = 5;
    private static WorkQueue instance = null;
    private final ThreadPoolExecutor workers;
    
    /**
     * Dummy object used to send messages from queue to main
     */
    public static Object messenger = new Object();
    
    private WorkQueue() {
        workers = new ThreadPoolExecutor(
                numThreads, 
                numThreads, 
                0, 
                TimeUnit.MILLISECONDS, 
                new LinkedBlockingQueue<Runnable>()
        );
    }

    public static WorkQueue getInstance() {
        if (instance == null) {
            synchronized (WorkQueue.class) {
                if (instance == null) {
                    instance = new WorkQueue();
                }
            }
        }
        return instance;
    }

    public void execute(Runnable r) {
        workers.execute(r);
    }

    public void shutdown() {
        workers.shutdown();
        // notify the main thread when work queue is shutting down
        synchronized (messenger) {
            messenger.notifyAll();
        }
    }
    
    public boolean isShutdown() {
        return workers.isShutdown();
    }
    
    public int getActiveCount() {
        return workers.getActiveCount();
    }
    
    public long getTaskCount() {
        return workers.getTaskCount();
    }
    
    /**
     * Blocks until work queue is shutdown
     */
    public void awaitShutdown() {
        while (!workers.isShutdown()) {
            synchronized (messenger) {
                try {
                    messenger.wait();
                } catch (InterruptedException e) {
                }
            }
            Thread.yield();
        }
    }
}

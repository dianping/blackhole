package com.dp.blackhole.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DaemonThreadFactory implements ThreadFactory{
    final AtomicInteger threadNumber = new AtomicInteger(1);
    final String namePrefix;
    
    public DaemonThreadFactory(String poolName) {
        namePrefix = poolName + "-thread-";
    }
    
    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
        t.setDaemon(true);
        return t;
    }

}

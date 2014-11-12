package com.dp.blackhole.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DaemonThreadFactory implements ThreadFactory{
    final AtomicInteger threadNumber = new AtomicInteger(1);
    final String namePrefix;
    static final String nameSuffix = "]";
    
    public DaemonThreadFactory(String poolName) {
        namePrefix = poolName + " Pool [Thread-";
    }
    
    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement()
                + nameSuffix);
        t.setDaemon(true);
        return t;
    }

}

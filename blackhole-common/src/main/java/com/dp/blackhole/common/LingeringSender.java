package com.dp.blackhole.common;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LingeringSender extends Thread {
    private static final Log LOG = LogFactory.getLog(LingeringSender.class);
    private volatile boolean running;
    public static final int DEFAULT_LINGER_MS = 10000;
    private static final int MINIMUM_LINGER_MS = 1000;
    private Set<Sender> senders;
    private AtomicInteger lingerMs = new AtomicInteger();
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock writeLock = lock.writeLock();
    private Lock readLock = lock.readLock();
    
    public LingeringSender() {
        this(DEFAULT_LINGER_MS);
    }
    
    public LingeringSender(int lingerMs) {
        this.senders = new HashSet<Sender>();
        this.running = true;
        if (lingerMs < MINIMUM_LINGER_MS) {
            this.lingerMs.set(MINIMUM_LINGER_MS);
            LOG.warn("lingerMs is less than " + MINIMUM_LINGER_MS
                    + "(MINIMUM_CHECK_INTERVAL) ,reset to it");
        } else {
            this.lingerMs.set(lingerMs);
        }
        setName("LingeringSender");
        setDaemon(true);
    }
    
    public void register(Sender sender) {
        writeLock.lock();
        try {
            senders.add(sender);
        } finally {
            writeLock.unlock();
        }
    }
    
    public void unregister(Sender sender) {
        writeLock.lock();
        try {
            senders.remove(sender);
        } finally {
            writeLock.unlock();
        }
    }
    
    public void shutdown() {
        running = false;
    }
    
    public int getLingerMs() {
        return lingerMs.get();
    }

    public void setLingerMs(int lingerMs) {
        this.lingerMs.set(lingerMs);;
    }

    @Override
    public void run() {
        LOG.info("LingeringSender start, lingerMs is " + lingerMs);
        Set<Sender> toRemove = new HashSet<Sender>();
        int heartbeatWaitTime = 0;
        while (running) {
            try {
                Thread.sleep(lingerMs.get());
                heartbeatWaitTime += 1;
            } catch (InterruptedException e) {
                LOG.error(this.getName() + this.getId() + " interrupt", e);
                running = false;
            }
            if (heartbeatWaitTime % 10 == 0) {
                heartbeatWaitTime = 0;
            }
            readLock.lock();
            try {
                for (Sender sender : senders) {
                    try {
                        if (sender.canSend()) {
                            sender.sendMessage();
                        } else {
                            if (heartbeatWaitTime == 0) {
                                sender.heartbeat();
                            }
                        }
                    } catch (IOException e) {
                        LOG.warn("Find socket " + sender.getSource() + "-->" + sender.getTarget() + " dead.");
                        toRemove.add(sender);
                    }
                }
            } finally {
                readLock.unlock();
                for (Sender sender : toRemove) {
                    sender.close();
                    unregister(sender);
                }
                toRemove.clear();
            }
        }
        senders.clear();
        LOG.info(this.getName() + this.getId() + " quit gracefully.");
    }
}

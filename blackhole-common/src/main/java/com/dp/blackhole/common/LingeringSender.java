package com.dp.blackhole.common;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LingeringSender extends Thread {
    private static final Log LOG = LogFactory.getLog(LingeringSender.class);
    private volatile boolean running;
    public static final int DEFAULT_LINGER_MS = 10000;
    private static final int MINIMUM_LINGER_MS = 1000;
    private ConcurrentHashMap<Sender, String> senders;
    private AtomicInteger lingerMs = new AtomicInteger();
    
    public LingeringSender() {
        this(DEFAULT_LINGER_MS);
    }
    
    public LingeringSender(int lingerMs) {
        this.senders = new ConcurrentHashMap<Sender, String>();
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
    
    public void register(String topic, String partitionId, Sender sender) {
        senders.putIfAbsent(sender, topic + ":" + partitionId);
    }
    
    public void unregister(Sender sender) {
        senders.remove(sender);
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
            for (Map.Entry<Sender, String> entry : senders.entrySet()) {
                Sender sender = entry.getKey();
                String connStr = entry.getValue();
                LOG.trace("sender checking..." + connStr);
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
                    sender.close();
                    senders.remove(sender);
                }
            }
        }
        senders.clear();
        LOG.info(this.getName() + this.getId() + " quit gracefully.");
    }
}

package com.dp.blackhole.common;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StreamHealthChecker extends Thread {
    private static final Log LOG = LogFactory.getLog(StreamHealthChecker.class);
    private volatile boolean running;
    private static final int DEFAULT_CHECK_INTERVAL = 5 * 60 * 1000;
    private static final int MINIMUM_CHECK_INTERVAL = 1000;
    private ConcurrentHashMap<Sender, String> senders;
    private AtomicInteger checkInteralMs = new AtomicInteger();
    private volatile boolean enableHeartbeat;
    
    public StreamHealthChecker() {
        this(DEFAULT_CHECK_INTERVAL, true);
    }
    
    public StreamHealthChecker(int checkInteralMs, boolean enableHeartbeat) {
        this.senders = new ConcurrentHashMap<Sender, String>();
        this.running = true;
        this.enableHeartbeat = enableHeartbeat;
        if (checkInteralMs < MINIMUM_CHECK_INTERVAL) {
            this.checkInteralMs.set(MINIMUM_CHECK_INTERVAL);
            LOG.warn("checkInteralMs is less than " + MINIMUM_CHECK_INTERVAL
                    + "(MINIMUM_CHECK_INTERVAL) ,reset to it");
        } else {
            this.checkInteralMs.set(checkInteralMs);
        }
        setName("StreamHealthChecker");
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
    
    public void enableHeartbeat() {
        LOG.info("enble StreamHealthChecker's heartbeat");
        this.enableHeartbeat = true;
    }
    
    public void disableHeartbeat() {
        LOG.info("disable StreamHealthChecker's heartbeat");
        this.enableHeartbeat = false;
    }

    public int getCheckInteralMs() {
        return checkInteralMs.get();
    }

    public void setCheckInteralMs(int checkInteralMs) {
        this.checkInteralMs.set(checkInteralMs);;
    }

    @Override
    public void run() {
        LOG.info("StreamHealthChecker start, checkInteralMs is " + checkInteralMs
                + ", current enableHeartbeat is " + enableHeartbeat);
        while (running) {
            try {
                Thread.sleep(checkInteralMs.get());
            } catch (InterruptedException e) {
                LOG.error(this.getName() + this.getId() + " interrupt", e);
                running = false;
            }
            for (Map.Entry<Sender, String> entry : senders.entrySet()) {
                Sender sender = entry.getKey();
                String connStr = entry.getValue();
                LOG.trace("sender checking..." + connStr);
                try {
                    if (sender.canSend()) {
                        sender.sendMessage();
                    } else {
                        if (enableHeartbeat) {
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

package com.dp.blackhole.common;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StreamHealthChecker extends Thread {
    private static final Log LOG = LogFactory.getLog(StreamHealthChecker.class);
    private volatile boolean running;
    private static final int CHECK_INTERVAL = 5 * 60 * 1000;
    private ConcurrentHashMap<Sender, String> senders;
    
    public StreamHealthChecker() {
        this.senders = new ConcurrentHashMap<Sender, String>();
        this.running = true;
        setDaemon(true);
        setName("StreamHealthChecker");
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

    @Override
    public void run() {
        while (running) {
            try {
                Thread.sleep(CHECK_INTERVAL);
            } catch (InterruptedException e) {
                LOG.error(this.getName() + this.getId() + " interrupt", e);
                running = false;
            }
            for (Map.Entry<Sender, String> entry : senders.entrySet()) {
                Sender sender = entry.getKey();
                String connStr = entry.getValue();
                LOG.debug("sender checking..." + connStr);
                try {
                    if (sender.canSend()) {
                        sender.sendMessage();
                    } else {
                        sender.heartbeat();
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

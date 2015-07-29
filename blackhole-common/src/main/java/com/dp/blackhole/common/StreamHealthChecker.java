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
    private ConcurrentHashMap<String, StreamConnection> connections;
    
    public StreamHealthChecker() {
        this.connections = new ConcurrentHashMap<String, StreamConnection>();
        this.running = true;
        setDaemon(true);
        setName("StreamHealthChecker");
    }
    
    public void register(String topic, String partitionId, StreamConnection connection) {
        connections.putIfAbsent(topic + ":" + partitionId, connection);
    }
    
    public void unregister(String topic, String partitionId) {
        connections.remove(topic + ":" + partitionId);
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
            for (Map.Entry<String, StreamConnection> entry : connections.entrySet()) {
                String key = entry.getKey();
                StreamConnection connection = entry.getValue();
                LOG.debug("connection checking..." + key);
                try {
                    if (connection.canSend()) {
                        connection.sendMessage();
                    } else {
                        connection.sendSignal();
                    }
                } catch (IOException e) {
                    LOG.warn("Find socket " + connection.getSource() + "-->" + connection.getTarget() + " dead.");
                    connection.close();
                    connections.remove(key);
                }
            }
        }
        connections.clear();
        LOG.info(this.getName() + this.getId() + " quit gracefully.");
    }
}

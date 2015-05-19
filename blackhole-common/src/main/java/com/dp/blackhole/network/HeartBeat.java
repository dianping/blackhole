package com.dp.blackhole.network;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.protocol.control.MessagePB.Message;

public class HeartBeat extends Thread {
    private static final Log LOG = LogFactory.getLog(HeartBeat.class);
    private static final int DEFAULT_HEARTBEAT_INTERVAL = 20000;
    private SimpleConnection connection;
    private volatile int interval;
    private volatile boolean running;
    private AtomicLong lastHeartBeat;
    
    public HeartBeat(SimpleConnection connection) {
        this(connection, DEFAULT_HEARTBEAT_INTERVAL);
    }
    
    public HeartBeat(SimpleConnection connection, int interval) {
        this.connection = connection;
        this.interval = interval;
        this.lastHeartBeat = new AtomicLong(Util.getTS());
        running = true;
        setDaemon(true);
    }
    
    @Override
    public void run() {
        Message heartbeat = PBwrap.wrapHeartBeat();
        while (running) {
            try {
                sleep(interval);
                Util.send(connection, heartbeat);
                if (connection != null) {
                    lastHeartBeat.getAndSet(Util.getTS());
                }
            } catch (InterruptedException e) {
                LOG.warn("HeartBeat interrupted", e);
                running = false;
            } catch (Throwable t) {
                LOG.error("Oops, catch an exception in HeartBeat, but go on.", t);
            }
        }
    }
    
    public void shutdown() {
        running = false;
    }
    
    public long getLastHeartBeat() {
        return lastHeartBeat.get();
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }
}
package com.dp.blackhole.network;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.protocol.control.MessagePB.Message;

public class HeartBeat extends Thread {
    private static final Log LOG = LogFactory.getLog(HeartBeat.class);
    private static final int DEFAULT_HEARTBEAT_INTERVAL = 20000;
    private ByteBufferNonblockingConnection connection;
    private volatile int interval;
    private volatile boolean running;
    private AtomicLong lastHeartBeat;
    private String version;
    
    public HeartBeat(ByteBufferNonblockingConnection connection) {
        this(connection, ParamsKey.DEFAULT_VERSION, DEFAULT_HEARTBEAT_INTERVAL);
    }
    
    public HeartBeat(ByteBufferNonblockingConnection connection, String version) {
        this(connection, version, DEFAULT_HEARTBEAT_INTERVAL);
    }
    
    public HeartBeat(ByteBufferNonblockingConnection connection, String version, int interval) {
        this.connection = connection;
        this.interval = interval;
        this.lastHeartBeat = new AtomicLong(Util.getTS());
        if (version == null) {
            this.version = ParamsKey.DEFAULT_VERSION;
        } else {
            this.version = version;
        }
        running = true;
        setDaemon(true);
    }
    
    @Override
    public void run() {
        Message heartbeat = PBwrap.wrapHeartBeat(this.version);
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
package com.dp.blackhole.network;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.protocol.control.MessagePB.Message;

public class HeartBeat extends Thread {
    private SimpleConnection connection;
    private int interval;
    private volatile boolean running;
    
    public HeartBeat(SimpleConnection connection) {
        this(connection, 5000);
    }
    
    public HeartBeat(SimpleConnection connection, int interval) {
        this.connection = connection;
        this.interval = interval;
        running = true;
        setDaemon(true);
    }
    
    @Override
    public void run() {
        Message heartbeat = PBwrap.wrapHeartBeat();
        while (running) {
            try {
                sleep(interval);
                connection.send(PBwrap.PB2Buf(heartbeat));
            } catch (InterruptedException e) {
            }
        }
    }
    
    public void shutdown() {
        running = false;
    }
}
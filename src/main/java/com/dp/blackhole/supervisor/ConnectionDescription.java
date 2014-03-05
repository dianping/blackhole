package com.dp.blackhole.supervisor;

import java.util.concurrent.atomic.AtomicLong;

import com.dp.blackhole.common.Util;

public class ConnectionDescription {
    public static final int AGENT = 1;
    public static final int BROKER = 2;
    public static final int CONSUMER = 3;
    
    private int type;
    private AtomicLong lastHeartBeat;
    
    public ConnectionDescription() {
        lastHeartBeat = new AtomicLong(Util.getTS());
    }
    
    public void setType(int type) {
        this.type = type;
    }
    
    public int getType() {
        return type;
    }
    
    public void updateHeartBeat() {
        lastHeartBeat.getAndSet(Util.getTS());
    }

    public long getLastHeartBeat() {
        return lastHeartBeat.get();
    }
}

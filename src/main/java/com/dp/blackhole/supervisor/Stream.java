package com.dp.blackhole.supervisor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import sun.misc.GC.LatencyRequest;

import com.dp.blackhole.common.Util;

public class Stream {
    String app;
    String appHost;
    private String collectorHost;
    long period;
    long startTs;
    AtomicLong lastSuccessTs = new AtomicLong();
    AtomicBoolean active = new AtomicBoolean(true);
    
    synchronized void setCollectorHost(String newCollectorHost) {
        collectorHost = newCollectorHost;
    }
    
    synchronized String getCollectorHost() {
        return collectorHost;
    }
    
    void setlastSuccessTs(long ts) {
        lastSuccessTs.getAndSet(ts);
    }
    
    void setGreatlastSuccessTs(long ts) {
        for (;;) {
            long current = lastSuccessTs.get();
            if (ts > current) {
                if (lastSuccessTs.compareAndSet(current, ts)) {
                    break;
                }
            } else {
                break;
            }
        }
    }
    
    long getlastSuccessTs() {
        return lastSuccessTs.get();
    }
    
    boolean isActive() {
        return active.get();
    }
    
    void updateActive(boolean status) {
        active.getAndSet(status);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((app == null) ? 0 : app.hashCode());
        result = prime * result + ((appHost == null) ? 0 : appHost.hashCode());
        result = prime * result + (int) (period ^ (period >>> 32));
        result = prime * result + (int) (startTs ^ (startTs >>> 32));
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Stream other = (Stream) obj;
        if (app == null) {
            if (other.app != null)
                return false;
        } else if (!app.equals(other.app))
            return false;
        if (appHost == null) {
            if (other.appHost != null)
                return false;
        } else if (!appHost.equals(other.appHost))
            return false;
        if (period != other.period)
            return false;
        if (startTs != other.startTs)
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return app+"@"+appHost+" period " + period+ " starttime "+Util.ts2String(startTs)+" lastSuccessTs "+lastSuccessTs.get()+" isActive "+isActive();
    } 
}

package com.dp.blackhole.supervisor.model;

import java.util.Date;

public class Issue {
    private String desc;
    private long ts;
    
    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return desc + " happened at " + new Date(ts) + "\n";
    }
}

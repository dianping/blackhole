package com.dp.blackhole.supervisor;

import java.util.Date;

public class Issue {
    public String desc;
    public long ts;
    
    @Override
    public String toString() {
        return desc + " happened at " + new Date(ts) + "\n";
    }
}

package com.dp.blackhole.supervisor;

import java.util.List;

public class Stage {
    public static final int APPEND = 1;
    public static final int UPLOADING = 3;
    public static final int UPLOAEDED = 4;
    public static final int RECOVERYING = 5;
    
    List<Issue> issuelist;
    
    String app;
    String apphost;
    String collectorhost;
    boolean cleanstart;
    int status;
    long rollTs;
    boolean isCurrent;
    
    public String getSummary() {
        return null;
    }

    public boolean isCurrent() {
        return isCurrent;
    }
}

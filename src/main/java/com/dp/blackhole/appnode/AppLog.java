package com.dp.blackhole.appnode;

public class AppLog {
    private String appName;
    private String tailFile;
    private long createTime;
    private String server;
    private int port;
    
    public AppLog(String appName, String tailFile) {
        this(appName, tailFile, System.currentTimeMillis());
    }
    
    public AppLog(String appName, String tailFile, long createTime) {
        this(appName, tailFile, createTime, null, 0);
    }
    
    public AppLog(String appName, String tailFile, long createTime, String server, int port) {
        this.appName = appName;
        this.tailFile = tailFile;
        this.createTime = createTime;
        this.server = server;
        this.port = port;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getTailFile() {
        return tailFile;
    }

    public void setTailFile(String tailFile) {
        this.tailFile = tailFile;
    }

    public long getCreateTime() {
        return createTime;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}

package com.dp.blackhole.appnode;

public class AppLog {
    private String appName;
    private String tailFile;
    private long createTime;
    private int bufSize;
    
    public AppLog(String appName, String tailFile, int bufSize) {
        this(appName, tailFile, System.currentTimeMillis(), bufSize);
    }
    
    public AppLog(String appName, String tailFile, long createTime, int bufSize) {
        this.appName = appName;
        this.tailFile = tailFile;
        this.createTime = createTime;
        this.bufSize = bufSize;
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

    public int getBufSize() {
        return bufSize;
    }

    public void setBufSize(int bufSize) {
        this.bufSize = bufSize;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((appName == null) ? 0 : appName.hashCode());
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
        AppLog other = (AppLog) obj;
        if (appName == null) {
            if (other.appName != null)
                return false;
        } else if (!appName.equals(other.appName))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "AppLog [appName=" + appName + ", tailFile=" + tailFile
                + ", createTime=" + createTime + "]";
    }
}

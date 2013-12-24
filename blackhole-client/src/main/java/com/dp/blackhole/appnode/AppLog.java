package com.dp.blackhole.appnode;

public class AppLog {
    private final String appName;
    private final String tailFile;
    private final long createTime;
    private int maxLineSize;
    
    public AppLog(String appName, String tailFile, int maxLineSize) {
        this(appName, tailFile, System.currentTimeMillis(), maxLineSize);
    }
    
    public AppLog(String appName, String tailFile, long createTime, int maxLineSize) {
        this.appName = appName;
        this.tailFile = tailFile;
        this.createTime = createTime;
        this.maxLineSize = maxLineSize;
    }

    public String getAppName() {
        return appName;
    }

    public String getTailFile() {
        return tailFile;
    }

    public long getCreateTime() {
        return createTime;
    }

    public int getMaxLineSize() {
        return maxLineSize;
    }

    public void setMaxLineSize(int maxLineSize) {
        this.maxLineSize = maxLineSize;
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
                + ", createTime=" + createTime + ", maxLineSize=" + maxLineSize
                + "]";
    }
}

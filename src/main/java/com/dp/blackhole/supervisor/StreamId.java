package com.dp.blackhole.supervisor;

public class StreamId {
    private String app;
    private String appHost;
    
    public StreamId(String app, String appHost) {
        this.app = app;
        this.appHost = appHost;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((app == null) ? 0 : app.hashCode());
        result = prime * result + ((appHost == null) ? 0 : appHost.hashCode());
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
        StreamId other = (StreamId) obj;
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
        return true;
    }
    
    @Override
    public String toString() {
        return app + "@" + appHost;
    }
    
}

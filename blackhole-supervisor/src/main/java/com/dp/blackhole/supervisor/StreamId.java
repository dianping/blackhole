package com.dp.blackhole.supervisor;

public class StreamId {
    private String app;
    private String sourceIdentify;
    
    public StreamId(String app, String sourceIdentify) {
        this.app = app;
        this.sourceIdentify = sourceIdentify;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((app == null) ? 0 : app.hashCode());
        result = prime * result + ((sourceIdentify == null) ? 0 : sourceIdentify.hashCode());
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
        if (sourceIdentify == null) {
            if (other.sourceIdentify != null)
                return false;
        } else if (!sourceIdentify.equals(other.sourceIdentify))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return app + "@" + sourceIdentify;
    }
    
    public boolean belongTo(String appName) {
        return app.equals(appName) ? true : false;
    }
}

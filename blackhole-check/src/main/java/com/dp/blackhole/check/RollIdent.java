package com.dp.blackhole.check;

import java.util.List;

public class RollIdent {
    public String app;
    public long period;
    public List<String> sources;
    public long ts;
    public boolean firstDeploy;
    public int timeout;
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((app == null) ? 0 : app.hashCode());
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
        RollIdent other = (RollIdent) obj;
        if (app == null) {
            if (other.app != null)
                return false;
        } else if (!app.equals(other.app))
            return false;
        return true;
    }
    @Override
    public String toString() {
        return "RollIdent [app=" + app + ", period=" + period + ", sources="
                + sources + ", ts=" + ts + "]";
    }
}

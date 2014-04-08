package com.dp.blackhole.scaleout;

import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class CmdbApp {

    private String cmdbAppname;
    private SortedSet<String> hostnames;
    
    public CmdbApp(String cmdbAppname) {
        this.cmdbAppname = cmdbAppname;
        this.hostnames = Collections.synchronizedSortedSet(new TreeSet<String>());
    }
    
    public CmdbApp(String cmdbAppname, List<String> hostnames) {
        this(cmdbAppname);
        for (String hostname : hostnames) {
            hostnames.add(hostname);
        }
    }
    
    public String getCmdbAppname() {
        return cmdbAppname;
    }
    
    public SortedSet<String> getHostnames() {
        return hostnames;
    }
    
    public void setHostnames(List<String> hostnames) {
        for (String hostname : hostnames) {
            this.hostnames.add(hostname);
        }
    }
    
    public void add(String hostname) {
        hostnames.add(hostname);
    }
}

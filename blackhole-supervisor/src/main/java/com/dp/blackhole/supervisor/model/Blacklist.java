package com.dp.blackhole.supervisor.model;

import java.util.List;

public class Blacklist {
    private List<String> blacklist;
    /**
     * @return blacklist may be null
     */
    public List<String> getBlacklist() {
        return blacklist;
    }
    public void setBlacklist(List<String> blacklist) {
        this.blacklist = blacklist;
    }
}

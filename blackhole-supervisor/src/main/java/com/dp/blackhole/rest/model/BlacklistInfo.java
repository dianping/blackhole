package com.dp.blackhole.rest.model;

import java.util.List;

public class BlacklistInfo {
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

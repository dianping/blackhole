package com.dp.blackhole.supervisor.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TopicConfig {
    private static int DEFAULT_MAX_LINE_SIZE = 512000;
    private static long DEFAULT_READ_INTERVAL = 1L;
    private static int DEFAULT_MIN_MSG_SENT = 30;
    private static int DEFAULT_MSG_BUF_SIZE = 512000;
    private String topic;
    private String appName;
    private String watchLog;
    private int rotatePeriod;
    private int rollPeriod;
    private int maxLineSize = DEFAULT_MAX_LINE_SIZE;
    private boolean isPersist = true;
    private List<String> hosts;
    private Map<String, Set<String>> hostsInstances;
    private long readInterval = DEFAULT_READ_INTERVAL;
    private String owner;
    private String compression;
    private int minMsgSent = DEFAULT_MIN_MSG_SENT;
    private int msgBufSize = DEFAULT_MSG_BUF_SIZE;
    
    public TopicConfig(String topic) {
        this.topic = topic;
    }
    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    public String getAppName() {
        return appName;
    }
    public void setAppName(String appName) {
        this.appName = appName;
    }
    public String getWatchLog() {
        return watchLog;
    }
    public void setWatchLog(String watchLog) {
        this.watchLog = watchLog;
    }
    public int getRotatePeriod() {
        return rotatePeriod;
    }
    public void setRotatePeriod(int rotatePeriod) {
        this.rotatePeriod = rotatePeriod;
    }
    public int getRollPeriod() {
        return rollPeriod;
    }
    public void setRollPeriod(int rollPeriod) {
        this.rollPeriod = rollPeriod;
    }
    public int getMaxLineSize() {
        return maxLineSize;
    }
    public void setMaxLineSize(int maxLineSize) {
        this.maxLineSize = maxLineSize;
    }
    public boolean isPersist() {
        return isPersist;
    }
    public void setPersist(boolean isPersist) {
        this.isPersist = isPersist;
    }
    public List<String> getHosts() {
        return hosts;
    }
    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }
    public long getReadInterval() {
        return readInterval;
    }
    public void setReadInterval(long readInterval) {
        this.readInterval = readInterval;
    }
    public synchronized Map<String, Set<String>> getInstances() {
        return hostsInstances;
    }
    public String getOwner() {
        return owner;
    }
    public void setOwner(String owner) {
        this.owner = owner;
    }
    public String getCompression() {
        return compression;
    }
    public void setCompression(String compression) {
        this.compression = compression;
    }
    public synchronized void setInstances(Map<String, Set<String>> hostsInstances) {
        this.hostsInstances = hostsInstances;
    }
    public Set<String> getInsByHost(String host) {
        if (hostsInstances != null) {
            return hostsInstances.get(host);
        } else {
            return null;
        }
    }
    public int getMinMsgSent() {
        return minMsgSent;
    }
    public void setMinMsgSent(int minMsgSent) {
        this.minMsgSent = minMsgSent;
    }
    public int getMsgBufSize() {
        return msgBufSize;
    }
    public void setMsgBufSize(int msgBufSize) {
        this.msgBufSize = msgBufSize;
    }
    public void addIdsByHosts(Map<String, Set<String>> hostIds) {
        if (getInstances() == null) {
            setInstances(hostIds);
        } else {
            for (Map.Entry<String, Set<String>> newHostIds : hostIds.entrySet()) {
                Set<String> oldIds = getInsByHost(newHostIds.getKey());
                if (oldIds == null) {
                    getInstances().put(newHostIds.getKey(), newHostIds.getValue());
                } else {
                    oldIds.addAll(newHostIds.getValue());
                }
            }
        }
    }
    public void removeIdsByHosts(Map<String, Set<String>> hostIds) {
        if (getInstances() != null) {
            for (Map.Entry<String, Set<String>> newHostIds : hostIds.entrySet()) {
                Set<String> oldIds = getInsByHost(newHostIds.getKey());
                if (oldIds != null) {
                    oldIds.removeAll(newHostIds.getValue());
                }
            }
        }
    }
}

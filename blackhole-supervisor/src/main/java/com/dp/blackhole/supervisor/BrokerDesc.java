package com.dp.blackhole.supervisor;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerDesc extends NodeDesc{
    private Map<String, ArrayList<String>> partitions;
    private int brokerPort;
    private int recoveryPort;
    
    public BrokerDesc(String id, int brokerPort, int recoveryPort) {
        super(id);
        partitions = new ConcurrentHashMap<String, ArrayList<String>>();
        this.brokerPort = brokerPort;
        this.recoveryPort = recoveryPort;
    }

    public void update(String topic, String partitionId) {
        ArrayList<String> plist = partitions.get(topic);
        if (plist == null) {
            plist = new ArrayList<String>();
            partitions.put(topic, plist);
        }
        synchronized (plist) {
            if (!plist.contains(partitionId)) {
                plist.add(partitionId);
            }
        }
    }
    
    public synchronized Map<String, ArrayList<String>> getPartitions() {
        return partitions;
    }
    
    public int getBrokerPort () {
        return brokerPort;
    }
    
    public int getRecoveryPort () {
        return recoveryPort;
    }
}

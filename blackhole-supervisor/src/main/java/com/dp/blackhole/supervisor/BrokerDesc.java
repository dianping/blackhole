package com.dp.blackhole.supervisor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BrokerDesc extends NodeDesc{
    private Map<String, ArrayList<String>> partitions;
    private int port;
    
    public BrokerDesc(String id, int port) {
        super(id);
        partitions = Collections.synchronizedMap(new HashMap<String, ArrayList<String>>());
        this.port = port;
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
    
    public int getPort () {
        return port;
    }
}

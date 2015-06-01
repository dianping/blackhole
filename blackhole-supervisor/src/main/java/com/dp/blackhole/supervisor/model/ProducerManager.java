package com.dp.blackhole.supervisor.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;

public class ProducerManager {
    private final Log LOG = LogFactory.getLog(ProducerManager.class);
    private static final boolean CONNECTED = true;
    private static final boolean DISCONNECTED = false;
    private HashMap<String, AtomicBoolean> producers;   //producerId -> is_connected
    private String topic;
    private int partitionFactor;
    
    private Map<String, PartitonState> partitionStateMap; //partitionId -> state
    
    public ProducerManager(String topic, int partitionFactor) {
        this.topic = topic;
        this.partitionFactor = partitionFactor;
        this.producers = new HashMap<String, AtomicBoolean>();
        this.partitionStateMap = new ConcurrentHashMap<String, PartitonState>();
    }
    
    public String getProducerId() {
        String producerId = null;
        synchronized (this) {
            if (producers.isEmpty()) {
                producerId = generateProducerId();
                LOG.info("new producer " + producerId + " generated.");
                producers.put(producerId, new AtomicBoolean(CONNECTED));
                return producerId;
            }
        }
        
        boolean findOffline = false;
        for (Map.Entry<String, AtomicBoolean> entry : producers.entrySet()) {
            producerId = entry.getKey();
            AtomicBoolean connected = entry.getValue();
            if (connected.compareAndSet(DISCONNECTED, CONNECTED)) {
                findOffline = true;
                LOG.info("old producer " + producerId + " reused.");
                break;
            }
        }
        
        if (!findOffline) {
            producerId = generateProducerId();
            LOG.info("new producer " + producerId + " generated.");
            producers.put(producerId, new AtomicBoolean(CONNECTED));
        }
        return producerId;
    }
    
    public int getPartitionFactor() {
        return partitionFactor;
    }

    public void setPartitionFactor(int partitionFactor) {
        this.partitionFactor = partitionFactor;
    }

    /**
     * generate random producerid ( uuid.sub(8) )
     *
     * @return random producerid
     */
    private String generateProducerId() {
        UUID uuid = UUID.randomUUID();
        return "p-" + Util.getTS() + "-" + Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8);
    }
    
    public List<String> generatePartitionId(String producerId) {
        List<String> partitionIds = getPartitionId(producerId);
        for (String partitionId : partitionIds) {
            partitionStateMap.put(partitionId, new PartitonState(partitionId));
        }
        return partitionIds;
    }
    
    public List<String> getPartitionId(String producerId) {
        List<String> partitionIds = new ArrayList<String>(partitionFactor);
        for (int i = 1; i <= partitionFactor; i++) { //partition index start from 1
            String partitinId = producerId + "#" + i;
            partitionIds.add(partitinId);
        }
        return partitionIds;
    }
    
    public boolean inactive(String producerId) {
        List<String> partitions = getPartitionId(producerId);
        for (String partitionId : partitions) {
            switchToOffline(partitionId);
        }
        AtomicBoolean isConnected;
        if ((isConnected = producers.get(producerId)) != null) {
            return isConnected.compareAndSet(CONNECTED, DISCONNECTED);
        } else {
            return false;
        }
    }
    
    public boolean switchToOnline(String partitionId) {
        PartitonState state;
        if((state = partitionStateMap.get(partitionId)) == null) {
            return false;
        } else {
            return state.switchToOnline();
        }
    }
    
    public boolean switchToOffline(String partitionId) {
        PartitonState state;
        if((state = partitionStateMap.get(partitionId)) == null) {
            return false;
        } else {
            return state.switchToOffline();
        }
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
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
        ProducerManager other = (ProducerManager) obj;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }
    
    static class PartitonState {
        private static final boolean OFFLINE = true;
        private static final boolean ONLINE = false;
        private final String partitionId;
        private AtomicBoolean offline;
        
        public PartitonState(String partitionId) {
            this.partitionId = partitionId;
            this.offline = new AtomicBoolean(OFFLINE);
        }
        
        public String getPartitionId() {
            return partitionId;
        }
        
        public boolean isOffline() {
            return offline.get();
        }
        
        boolean switchToOnline() {
            return offline.compareAndSet(OFFLINE, ONLINE);
        }
        
        boolean switchToOffline() {
            offline.set(OFFLINE);
            return true;
        }
    }
}

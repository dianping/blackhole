package com.dp.blackhole.agent.persist;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.agent.TopicMeta.MetaKey;

public class LocalState implements IState {
    private static final Log LOG = LogFactory.getLog(LocalState.class);
    private String persistDir;
    private ConcurrentHashMap<MetaKey, Snapshot> snapshotMap;
    
    LocalState(String persistDir) {
        this.persistDir = persistDir;
        this.snapshotMap = new ConcurrentHashMap<MetaKey, Snapshot>();//TODO should use concurrent?
    }
    
    @Override
    public void record(MetaKey key, String stage, Record record) {
        Snapshot snapshot = snapshotMap.get(key);
        if (snapshot == null) {
            snapshot = new Snapshot(key, stage);
        }
        snapshot.addRecord(record);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean isComplete() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object snapshot() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void persist(Object o) {
        // TODO Auto-generated method stub
        
    }

}

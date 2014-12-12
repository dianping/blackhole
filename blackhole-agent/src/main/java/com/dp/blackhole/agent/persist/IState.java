package com.dp.blackhole.agent.persist;

import com.dp.blackhole.agent.TopicMeta.MetaKey;

public interface IState {

    public void record(MetaKey key, String stage, Record record);
    
    public void cleanup();
    
    public boolean isComplete();
    
    public Object snapshot();
    
    public void persist(Object o);
}

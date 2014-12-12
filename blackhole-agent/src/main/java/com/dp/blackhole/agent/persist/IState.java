package com.dp.blackhole.agent.persist;

public interface IState {
    
    public void record(int type, long rollTs, long endOffset);
    
    public void record(int type, long rollTs, long startOffset, long endOffset);
    
    public Record retrive(long rollTs);
    
    public Record retriveLastRollRecord();
    
    public Record retriveLastRecord(int type);
    
    public Record retriveFirstRecord();
    
    public void cleanup();
    
    public boolean isComplete();

}

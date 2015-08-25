package com.dp.blackhole.agent.persist;

public interface IRecoder {
    
    public void record(int type, long rollTs, long currentRotation, long startOffset, long endOffset);
    
    public Record getPerviousRollRecord();
    
    public Record retrive(long rollTs);
    
    public Record retriveLastRecord(int type);
    
    public Record retriveFirstRecord();
    
    public void tidy();
    
    public boolean isComplete();
    
    public void log();

    public void cleanup();

}

package com.dp.blackhole.supervisor;


public class BrokerDesc extends NodeDesc{
    private int brokerPort;
    private int recoveryPort;
    
    public BrokerDesc(String id, int brokerPort, int recoveryPort) {
        super(id);
        this.brokerPort = brokerPort;
        this.recoveryPort = recoveryPort;
    }
    
    public int getBrokerPort () {
        return brokerPort;
    }
    
    public int getRecoveryPort () {
        return recoveryPort;
    }
}

package com.dp.blackhole.supervisor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.SimpleConnection;

public class ConnectionDescription {
    public static final int AGENT = 1;
    public static final int BROKER = 2;
    public static final int CONSUMER = 3;
    
    private int type;
    private AtomicLong lastHeartBeat;
    private SimpleConnection connection;
    private List<NodeDesc> attachments;
    
    public ConnectionDescription(SimpleConnection connection) {
        this.connection = connection;
        lastHeartBeat = new AtomicLong(Util.getTS());
        attachments = new ArrayList<NodeDesc>();
    }
    
    public void setType(int type) {
        this.type = type;
    }
    
    public int getType() {
        return type;
    }
    
    public void updateHeartBeat() {
        lastHeartBeat.getAndSet(Util.getTS());
    }

    public long getLastHeartBeat() {
        return lastHeartBeat.get();
    }
    
    public SimpleConnection getConnection() {
        return connection;
    }
    
    public List<NodeDesc> getAttachments() {
        return attachments;
    }

    public void attach(NodeDesc desc) {
        attachments.add(desc);
    }
    
    @Override
    public String toString() {
        String typeName = "";
        switch (this.type) {
        case AGENT:
            typeName = "AGENT";
            break;
        case BROKER:
            typeName = "BROKER";
            break;
        case CONSUMER:
            typeName = "CONSUMER";
            break;
        default:
            typeName = "UNKNOW";
            break;
        }
        return connection.toString() + " type: " + typeName;
    }
}

package com.dp.blackhole.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AgentProtocol {
    
    public class AgentHead {
        public boolean ignore;
        public String app;
        public String source;
        public long peroid;
        public long ts;
        public long size;
        public boolean hasCompressed;
        public boolean isFinal = false;
    }
    
    public DataOutputStream sendHead (DataOutputStream out, AgentHead head) throws IOException {
        out.writeBoolean(head.ignore);
        Util.writeString(head.app, out);
        Util.writeString(head.source, out);
        out.writeLong(head.peroid);
        out.writeLong(head.ts);
        out.writeLong(head.size);
        out.writeBoolean(head.hasCompressed);
        out.writeBoolean(head.isFinal);
        return out;
    }
    
    public AgentHead recieveHead (DataInputStream in, AgentHead head) throws IOException {
        head.ignore = in.readBoolean();
        head.app = Util.readString(in);
        head.source = Util.readString(in);
        head.peroid = in.readLong();
        head.ts = in.readLong();
        head.size = in.readLong();
        head.hasCompressed = in.readBoolean();
        head.isFinal = in.readBoolean();
        return head;
    }
}

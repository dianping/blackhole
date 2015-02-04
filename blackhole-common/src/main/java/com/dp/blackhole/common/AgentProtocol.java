package com.dp.blackhole.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AgentProtocol {
    
    public class AgentHead {
        public boolean ignore;
        public String app;
        public String source;
        public long period;
        public long ts;
        public long size;
        public boolean hasCompressed;
        public boolean isFinal = false;
        public boolean isPersist = true;
    }
    
    public DataOutputStream sendHead (DataOutputStream out, AgentHead head) throws IOException {
        out.writeBoolean(head.ignore);
        Util.writeString(head.app, out);
        Util.writeString(head.source, out);
        out.writeLong(head.period);
        out.writeLong(head.ts);
        out.writeLong(head.size);
        out.writeBoolean(head.hasCompressed);
        out.writeBoolean(head.isFinal);
        out.writeBoolean(head.isPersist);
        return out;
    }
    
    public AgentHead recieveHead (DataInputStream in, AgentHead head) throws IOException {
        head.ignore = in.readBoolean();
        head.app = Util.readString(in);
        head.source = Util.readString(in);
        head.period = in.readLong();
        head.ts = in.readLong();
        head.size = in.readLong();
        head.hasCompressed = in.readBoolean();
        head.isFinal = in.readBoolean();
        head.isPersist = in.readBoolean();
        return head;
    }
}

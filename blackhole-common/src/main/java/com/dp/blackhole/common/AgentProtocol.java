package com.dp.blackhole.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AgentProtocol {
    public static final int STREAM = 0x1;
    public static final int RECOVERY = 0x2;
    public static final int IN_PAAS = 0x4;
    public static final int STREAM_IN_PAAS = STREAM | IN_PAAS;
    public static final int RECOVERY_IN_PAAS = RECOVERY | IN_PAAS;
  
    public class AgentHead {
        public int type;
        public String app;
        public String instanceId;
        public long peroid;
        public long ts;
        public long size;
        public boolean hasCompressed;
        public boolean isFinal = false;
    }
    
    public DataOutputStream sendHead (DataOutputStream out, AgentHead head) throws IOException {

        out.writeInt(head.type);
        Util.writeString(head.app, out);
        if ((head.type & IN_PAAS) != 0) {
            Util.writeString(head.instanceId, out);
            out.writeBoolean(head.isFinal);
        }
        out.writeLong(head.peroid);

        if ((head.type & RECOVERY) == RECOVERY) {
            out.writeLong(head.ts);
            out.writeLong(head.size);
            out.writeBoolean(head.hasCompressed);
        }

        return out;
    }
    
    public AgentHead recieveHead (DataInputStream in, AgentHead head) throws IOException {
        head.type = in.readInt();
        head.app = Util.readString(in);
        if ((head.type & IN_PAAS) != 0) {
            head.instanceId = Util.readString(in);
            head.isFinal = in.readBoolean();
        }
        head.peroid = in.readLong();
        if ((head.type & RECOVERY) == RECOVERY) {
            head.ts = in.readLong();
            head.size = in.readLong();
            head.hasCompressed = in.readBoolean();
        }
        return head;
    }
}

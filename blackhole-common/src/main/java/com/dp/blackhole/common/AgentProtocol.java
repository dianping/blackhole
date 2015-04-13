package com.dp.blackhole.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AgentProtocol {
    public static final int VERSION_MICOR_BATCH = 0x8;
    // for old version
    public static final int STREAM = 0x1;
    public static final int RECOVERY = 0x2;
    public static final int IN_PAAS = 0x4;
    public static final int STREAM_IN_PAAS = STREAM | IN_PAAS;
    public static final int RECOVERY_IN_PAAS = RECOVERY | IN_PAAS;
    public class AgentHead {
        public int version;
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
        out.writeInt(head.version);
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
    
    /**
     * for compatibility
     */
    public AgentHead recieveHead (DataInputStream in, AgentHead head) throws IOException {
        head.version = in.readInt();
        if (head.version == VERSION_MICOR_BATCH) {
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
        } else {
            head.app = Util.readString(in);
            if ((head.version & IN_PAAS) != 0) {
                head.source = Util.readString(in);
                head.isFinal = in.readBoolean();
            }
            head.period = in.readLong();
            if ((head.version & RECOVERY) == RECOVERY) {
                head.ts = in.readLong();
                head.size = in.readLong();
                head.hasCompressed = in.readBoolean();
            }
            return head;
        }
    }
}

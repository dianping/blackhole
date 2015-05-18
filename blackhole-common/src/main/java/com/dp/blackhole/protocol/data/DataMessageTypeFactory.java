package com.dp.blackhole.protocol.data;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.network.TypedWrappable;
import com.dp.blackhole.network.TypedFactory;

public class DataMessageTypeFactory implements TypedFactory {
    public static final Log LOG = LogFactory.getLog(DataMessageTypeFactory.class);
    
    public static final int VersionRequest = 0;
    public static final int ProduceRequest = 1;
    public static final int FetchRequest = 2;
    public static final int FetchReply = 3;
    public static final int MultiFetchRequest = 4;
    public static final int MultiFetchReply = 5;
    public static final int OffsetRequest = 6;
    public static final int OffsetReply = 7;
    public static final int RotateOrRollRequest = 8;
    public static final int RegisterRequest = 9;
    public static final int LastRotateRequest = 10;
    
    @Override
    public TypedWrappable getWrappedInstanceFromType(int type) {
        TypedWrappable ret = null;
        switch (type) {
        case 0:
            ret = new VersionRequest();
            break;
        case 1:
            ret = new ProduceRequest();
            break;
        case 2:
            ret = new FetchRequest();
            break;
        case 3:
            ret = new FetchReply();
            break;
        case 4:
            ret = new MultiFetchRequest();
            break;
        case 5:
            ret = new MultiFetchReply();
            break;
        case 6:
            ret = new OffsetRequest();
            break;
        case 7:
            ret = new OffsetReply();
            break;
        case 8:
            ret = new RollRequest();
            break;
        case 9:
            ret = new RegisterRequest();
            break;
        case 10:
            ret = new HaltRequest();
            break;
        default:
            LOG.error("unknown Message Type: " + type);
        }
        return ret;
    }
}

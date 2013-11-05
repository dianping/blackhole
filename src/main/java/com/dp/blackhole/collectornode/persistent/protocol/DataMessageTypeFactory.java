package com.dp.blackhole.collectornode.persistent.protocol;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.network.TypedWrappable;
import com.dp.blackhole.network.TypedFactory;

public class DataMessageTypeFactory implements TypedFactory {
    public static final Log LOG = LogFactory.getLog(DataMessageTypeFactory.class);
    
    public static final int produceRequest = 1;
    public static final int FetchRequest = 2;
    public static final int FetchReply = 3;
    
    @Override
    public TypedWrappable getWrappedInstanceFromType(int type) {
        TypedWrappable ret = null;
        switch (type) {
        case 1:
            ret = new FetchReply();
            break;
        default:
            LOG.error("unknown Message Type: " + type);
        }
        return ret;
    }
}

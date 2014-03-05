package com.dp.blackhole.simutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.appnode.Appnode;

public class SimAppnode extends Appnode{
    private static final Log LOG = LogFactory.getLog(SimAppnode.class);
    public SimAppnode(String appClient, int port) {
        super(appClient);
        setPort(port);
    }

    private void setPort(int port) {
        super.recoveryPort = port;
    }

    @Override
    public void reportFailure(String app, String appHost, long ts) {
        LOG.debug("APP: " + app + ", APP HOST: " + appHost + "ts: " + ts);
    }
    
    public void reportUnrecoverable(String appName, String appHost, long ts) {
        LOG.debug("APP: " + appName + ", APP HOST: " + appHost + "roll ts: " + ts);
    }
}

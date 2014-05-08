package com.dp.blackhole.broker;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.broker.Broker;
import com.dp.blackhole.broker.RollIdent;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.protocol.control.MessagePB.Message;

public class SimCollectornode extends Broker {
    private static final Log LOG = LogFactory.getLog(SimCollectornode.class);
    public static String HOSTNAME;
    public static long rollTS = 1357023691855l;
    public static final String SCHEMA = "file://";
    public static final String BASE_PATH = "/tmp/hdfs/";
    public static final String BASE_HDFS_PATH = SCHEMA + BASE_PATH;
    public static final String FILE_SUFFIX = "2013-01-01.15";
    public static final String expected = " 0f j2390jr092jf2f02jf02qjdf2-3j0 fiopwqejfjwffhg5_p    <end";
    private int port;
    static {
        try {
                HOSTNAME = Util.getLocalHost();
            } catch (UnknownHostException e) {
            }
        }
    public SimCollectornode(int port) throws IOException {
        super();
        this.port = port;
    }
    
    @Override
    public void send(Message msg) {
        LOG.debug(msg.getType());
    }
    
    public void start() {
        try {
            Broker.getRollMgr().init("/tmp/hdfs", ".gz", port, 5000, 1, 1, 60000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void deleteTmpFile(String MAGIC) {
        File dir = new File("/tmp");
        for (File file : dir.listFiles()) {
            if (file.getName().contains(MAGIC)) {
                LOG.debug("delete tmp file " + file);
                file.delete();
            }
        }
    }

    public static RollIdent getRollIdent(String appName) {
        RollIdent rollIdent = new RollIdent();
        rollIdent.app = appName;
        rollIdent.period = 3600;
        rollIdent.source = SimCollectornode.HOSTNAME;
        rollIdent.ts = SimCollectornode.rollTS;
        return rollIdent;
    }
}

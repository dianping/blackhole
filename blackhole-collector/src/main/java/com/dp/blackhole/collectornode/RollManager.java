package com.dp.blackhole.collectornode;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dp.blackhole.collectornode.persistent.RollPartition;
import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.RollIDPB.RollID;

public class RollManager {
    private final static Log LOG = LogFactory.getLog(RollManager.class);
    
    Map<RollIdent, RollPartition> rolls;
    
    private String hdfsbase;
    private String suffix;
    private int port;
    private FileSystem fs;
    private ExecutorService pool;
    private RecoveryAcceptor accepter;
    private long clockSyncBufMillis;
    
    public void init(String hdfsbase, String suffix, int port, long clockSyncBufMillis) throws IOException {
        this.hdfsbase = hdfsbase;
        this.suffix = suffix;
        this.port = port;
        this.clockSyncBufMillis = clockSyncBufMillis;
        pool = Executors.newCachedThreadPool();
        fs = (new Path(hdfsbase)).getFileSystem(new Configuration());
        rolls = Collections.synchronizedMap(new HashMap<RollIdent, RollPartition>());
        accepter = new RecoveryAcceptor();
        accepter.start();
        LOG.info("roll manager started");
    }
    
    public boolean doRegister(String app, String source, long period, RollPartition roll) {
        boolean ret;
        RollIdent ident = getRollIdent(app, source, period);
        if (rolls.get(ident) == null) {
            rolls.put(ident, roll);
            Message message = PBwrap.wrapAppRoll(ident.app, ident.source, ident.period, ident.ts);
            Broker.getSupervisor().send(message);
            ret = true;
        } else {
            LOG.fatal("register a exists roll: " + ident);
            ret =false;
        }
        return ret;
    }
    
    public boolean doUpload(RollID rollID) {
        RollIdent ident = new RollIdent();
        ident.app = rollID.getAppName();
        ident.source = rollID.getAppServer();
        ident.period = rollID.getPeriod();
        ident.ts = rollID.getRollTs();
        
        RollPartition roll = rolls.get(ident);
        
        if (roll == null) {
            LOG.error("can not find roll by rollident " + ident);
            reportUpload(ident, false);
            return false;
        }
        
        HDFSUpload upload = new HDFSUpload(this, Broker.getBrokerService().getPersistentManager(), fs, ident, roll);
        pool.execute(upload);
        return true;
    }
    
    private RollIdent getRollIdent(String app, String source, long period) {
        Date time = new Date(Util.getLatestRotateRollTsUnderTimeBuf(Util.getTS(), period, clockSyncBufMillis));
        RollIdent roll = new RollIdent();
        roll.app = app;
        roll.source = source;
        roll.period = period;
        roll.ts = time.getTime();
        return roll;
    }
    
    private String getDatepathbyFormat (String format) {
        StringBuilder dirs = new StringBuilder();
        for (String dir: format.split("\\.")) {
            dirs.append(dir);
            dirs.append('/');
        }
        return dirs.toString();
    }
    
    /*
     * Path format:
     * hdfsbasedir/appname/2013-11-01/14/08/machine01@appname_2013-11-01.14.08.gz.tmp
     */
    private String getRollHdfsPathPrefix (RollIdent ident) {
        String format;
        format = Util.getFormatFromPeroid(ident.period);
        Date roll = new Date(ident.ts);
        SimpleDateFormat dm= new SimpleDateFormat(format);
        return hdfsbase + '/' + ident.app + '/' + getDatepathbyFormat(dm.format(roll)) + 
                ident.source + '@' + ident.app + "_" + dm.format(roll);
    }
    
    public String getRollHdfsPath (RollIdent ident) {
        return getRollHdfsPathPrefix(ident) + suffix;
    }
    
    
    public void reportRecovery(RollIdent ident, boolean recoverySuccess) {
        Message message;
        if (recoverySuccess == true) {
            message = PBwrap.wrapRecoverySuccess(ident.app, ident.source, ident.ts);
        } else {
            message = PBwrap.wrapRecoveryFail(ident.app, ident.source, ident.ts);
        }
        Broker.getSupervisor().send(message);
    }

    public void reportUpload(RollIdent ident, boolean uploadSuccess) {
        rolls.remove(ident);
        
        if (uploadSuccess == true) {
            Message message = PBwrap.wrapUploadSuccess(ident.app, ident.source, ident.ts);
            Broker.getSupervisor().send(message);
        } else {
            Message message = PBwrap.wrapUploadFail(ident.app, ident.source, ident.ts);
            Broker.getSupervisor().send(message);
        }
    }

    public void reportFailure(String app, String appHost, long ts) {
        Message message = PBwrap.wrapcollectorFailure(app, appHost, ts);
        Broker.getSupervisor().send(message);
    }
    
    public void close() {
        LOG.info("shutdown collector node");
        pool.shutdownNow();
        try {
            accepter.close();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        try {
            fs.close();
        } catch (IOException e) {
            LOG.error("error close ServerSocket", e);
        }
    }
    
    private class RecoveryAcceptor extends Thread {
        private boolean running = true;
        private ServerSocket server;
        
        public RecoveryAcceptor() throws IOException {
            server = new ServerSocket(port);
        }
        
        public void close() throws IOException {
            server.close();
        }
        
        @Override
        public void run() {
            while (running) {
                try {
                    Socket socket = server.accept();
                    
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    
                    AgentProtocol protocol = new AgentProtocol();
                    AgentHead head = protocol.new AgentHead();
                    
                    protocol.recieveHead(in, head);
                        
                    RollIdent roll = new RollIdent();
                    roll.app = head.app;
                    roll.source = Util.getRemoteHost(socket);
                    roll.period = head.peroid;
                    roll.ts = head.ts;

                    HDFSRecovery recovery = new HDFSRecovery(
                            RollManager.this, fs, socket, roll);
                    pool.execute(recovery);

                } catch (IOException e) {
                    LOG.error("error in acceptor: ", e);
                }
            }
        }
    }
}

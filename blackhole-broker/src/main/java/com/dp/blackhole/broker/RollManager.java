package com.dp.blackhole.broker;

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

import com.dp.blackhole.broker.ftp.FTPConfigration;
import com.dp.blackhole.broker.ftp.FTPConfigrationLoader;
import com.dp.blackhole.broker.ftp.FTPUpload;
import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.RollPartition;
import com.dp.blackhole.broker.storage.StorageManager;
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
    private ExecutorService uploadPool;
    private ExecutorService recoveryPool;
    private RecoveryAcceptor accepter;
    private long clockSyncBufMillis;
    
    public void init(String hdfsbase, String suffix, int port, long clockSyncBufMillis, 
            int maxUploadThreads, int maxRecoveryThreads, int recoverySocketTimeout) throws IOException {
        this.hdfsbase = hdfsbase;
        this.suffix = suffix;
        this.port = port;
        this.clockSyncBufMillis = clockSyncBufMillis;
        uploadPool = Executors.newFixedThreadPool(maxUploadThreads);
        recoveryPool = Executors.newFixedThreadPool(maxRecoveryThreads);
        fs = (new Path(hdfsbase)).getFileSystem(new Configuration());
        rolls = Collections.synchronizedMap(new HashMap<RollIdent, RollPartition>());
        accepter = new RecoveryAcceptor(recoverySocketTimeout);
        accepter.start();
        LOG.info("roll manager started");
    }
    
    public boolean doRegister(String app, String source, long period, RollPartition roll) {
        boolean ret;
        RollIdent ident = getRollIdent(app, source, period);
        if (rolls.get(ident) == null) {
            rolls.put(ident, roll);
            Message message = PBwrap.wrapAppRoll(ident.topic, ident.sourceIdentify, ident.period, ident.ts);
            Broker.getSupervisor().send(message);
            ret = true;
        } else {
            LOG.fatal("register a exists roll: " + ident);
            ret =false;
        }
        return ret;
    }
    
    public boolean doClean(String app, String source, long period, RollPartition roll) {
        boolean ret;
        RollIdent ident = getLastRollIdent(app, source, period);
        if (rolls.get(ident) == null) {
            rolls.put(ident, roll);
            Message message = PBwrap.wrapRollClean(ident.topic, ident.sourceIdentify, ident.period);
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
        ident.topic = rollID.getTopic();
        ident.sourceIdentify = rollID.getSourceIdentify();
        ident.period = rollID.getPeriod();
        ident.ts = rollID.getRollTs();
        ident.isFinal = rollID.getIsFinal();
        
        RollPartition roll = rolls.get(ident);
        
        if (roll == null) {
            LOG.error("can not find roll by rollident " + ident);
            reportUpload(ident, false);
            return false;
        }
        
        LOG.info("start to upload roll " + ident);
        StorageManager manager = Broker.getBrokerService().getPersistentManager();
        HDFSUpload upload = new HDFSUpload(this, manager, fs, ident, roll);
        uploadPool.execute(upload);
        //start a ftp uploader which need uploading to work with HDFS in parallel.
        FTPConfigration ftpConf;
        if ((ftpConf = FTPConfigrationLoader.getFTPConfigration(ident.topic)) != null) {
            Partition p;
            try {
                p = manager.getPartition(ident.topic, ident.sourceIdentify, false);
                LOG.info("start to ftp " + ident);
                FTPUpload ftpUpload = new FTPUpload(this, ftpConf, ident, roll, p);
                Thread ftpThread = new Thread(ftpUpload);
                ftpThread.start();
            } catch (IOException e) {
                LOG.error("Can not get a partition to upload with FTP of ident: " + ident, e);
            }
        }
        return true;
    }

    public void markUnrecoverable(RollID rollID) {
        RollIdent ident = new RollIdent();
        ident.topic = rollID.getTopic();
        ident.sourceIdentify = rollID.getSourceIdentify();
        ident.period = rollID.getPeriod();
        ident.ts = rollID.getRollTs();
        
        LOG.info("start to mark unrecoverable roll " + ident);
        HDFSMarker marker = new HDFSMarker(this, fs, ident);
        uploadPool.execute(marker);
    }
    
    private RollIdent getRollIdent(String app, String source, long period) {
        Date time = new Date(Util.getLatestRotateRollTsUnderTimeBuf(Util.getTS(), period, clockSyncBufMillis));
        RollIdent roll = new RollIdent();
        roll.topic = app;
        roll.sourceIdentify = source;
        roll.period = period;
        roll.ts = time.getTime();
        return roll;
    }
    
    private RollIdent getLastRollIdent(String app, String source, long period) {
        Date time = new Date(Util.getCurrentRollTs(Util.getTS(), period));
        RollIdent roll = new RollIdent();
        roll.topic = app;
        roll.sourceIdentify = source;
        roll.period = period;
        roll.ts = time.getTime();
        roll.isFinal = true;
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
     * hdfsbasedir/appname/2013-11-01/14/08/
     */
    public String getParentPath(String baseDir, RollIdent ident) {
        String format;
        format = Util.getFormatFromPeroid(ident.period);
        Date roll = new Date(ident.ts);
        SimpleDateFormat dm = new SimpleDateFormat(format);
        return baseDir + "/" + ident.topic + '/' + getDatepathbyFormat(dm.format(roll));
    }
    
    /*
     * machine01@appname_2013-11-01.14.08
     */
    public String getFileName(RollIdent ident, boolean hidden) {
        String format;
        format = Util.getFormatFromPeroid(ident.period);
        Date roll = new Date(ident.ts);
        SimpleDateFormat dm = new SimpleDateFormat(format);
        if (hidden) {
            return "_" + ident.sourceIdentify + '@' + ident.topic + "_" + dm.format(roll);
        } else {
            return ident.sourceIdentify + '@' + ident.topic + "_" + dm.format(roll);
        }
    }
    
    public String getCompressedFileName(RollIdent ident) {
        return getFileName(ident, false) + suffix;
    }

    /*
     * Path format:
     * hdfsbasedir/appname/2013-11-01/14/08/machine01@appname_2013-11-01.14.08.gz
     */
    public String getRollHdfsPath(RollIdent ident) {
        return getParentPath(hdfsbase, ident) + getCompressedFileName(ident);
    }
    
    public String getMarkHdfsPath(RollIdent ident) {
        return getParentPath(hdfsbase, ident) + getFileName(ident, true);
    }
    
    public void reportRecovery(RollIdent ident, boolean recoverySuccess) {
        Message message;
        if (recoverySuccess == true) {
            message = PBwrap.wrapRecoverySuccess(ident.topic, ident.sourceIdentify, ident.period, ident.ts, ident.isFinal);
        } else {
            message = PBwrap.wrapRecoveryFail(ident.topic, ident.sourceIdentify, ident.period, ident.ts, ident.isFinal);
        }
        Broker.getSupervisor().send(message);
    }

    public void reportUpload(RollIdent ident, boolean uploadSuccess) {
        rolls.remove(ident);
        
        if (uploadSuccess == true) {
            Message message = PBwrap.wrapUploadSuccess(ident.topic, ident.sourceIdentify, ident.period, ident.ts, ident.isFinal);
            Broker.getSupervisor().send(message);
        } else {
            Message message = PBwrap.wrapUploadFail(ident.topic, ident.sourceIdentify, ident.period, ident.ts, ident.isFinal);
            Broker.getSupervisor().send(message);
        }
    }

    public void reportFailure(String app, String appHost, long ts) {
        Message message = PBwrap.wrapBrokerFailure(app, appHost, ts);
        Broker.getSupervisor().send(message);
    }
    
    public void close() {
        LOG.info("shutdown broker node");
        uploadPool.shutdownNow();
        recoveryPool.shutdownNow();
        try {
            accepter.close();
        } catch (IOException e1) {
            LOG.error(e1.getMessage());
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
        private int recoverySocketTimeout;
        
        public RecoveryAcceptor(int recoverySocketTimeout) throws IOException {
            this.recoverySocketTimeout = recoverySocketTimeout;
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
                    socket.setSoTimeout(recoverySocketTimeout);
                    
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    
                    AgentProtocol protocol = new AgentProtocol();
                    AgentHead head = protocol.new AgentHead();
                    
                    protocol.recieveHead(in, head);
                        
                    RollIdent roll = new RollIdent();
                    roll.topic = head.app;
                    roll.sourceIdentify = Util.getRemoteHost(socket);
                    if (head.instanceId != null) {
                        roll.sourceIdentify += "#" + head.instanceId;
                    }
                    roll.period = head.peroid;
                    roll.ts = head.ts;
                    roll.isFinal = head.isFinal;
                    
                    LOG.info("start to recovery roll " + roll);
                    
                    HDFSRecovery recovery = new HDFSRecovery(
                            RollManager.this, fs, socket, roll, head.size, head.hasCompressed);
                    recoveryPool.execute(recovery);

                } catch (IOException e) {
                    LOG.error("error in acceptor: ", e);
                }
            }
        }
    }
}

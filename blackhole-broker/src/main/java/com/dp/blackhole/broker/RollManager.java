package com.dp.blackhole.broker;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dp.blackhole.broker.Compression.Algorithm;
import com.dp.blackhole.broker.ftp.FTPConfigration;
import com.dp.blackhole.broker.ftp.FTPConfigrationLoader;
import com.dp.blackhole.broker.ftp.FTPUpload;
import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.RollPartition;
import com.dp.blackhole.broker.storage.StorageManager;
import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.RollIDPB.RollID;

public class RollManager {
    private final static Log LOG = LogFactory.getLog(RollManager.class);
    private static final String TMP_SUFFIX = ".tmp";
    private static final String R_SUFFIX = ".r";
    private ConcurrentHashMap<RollIdent, RollPartition> rolls;
    private String hdfsbase;
    private String defaultCompression;
    private int port;
    private FileSystem fs;
    private ExecutorService uploadPool;
    private ExecutorService recoveryPool;
    private RecoveryAcceptor accepter;
    private long clockSyncBufMillis;
    private Algorithm defaultCompressionAlgo;
    
    public Algorithm getDefaultCompressionAlgo() {
        return defaultCompressionAlgo;
    }
    
    public void init(String hdfsbase, String defaultCompression, int port, long clockSyncBufMillis, 
            int maxUploadThreads, int maxRecoveryThreads, int recoverySocketTimeout) throws IOException {
        this.hdfsbase = hdfsbase;
        this.defaultCompression = defaultCompression;
        this.port = port;
        this.clockSyncBufMillis = clockSyncBufMillis;
        this.defaultCompressionAlgo = Compression.getCompressionAlgorithmByName(defaultCompression);
        uploadPool = Executors.newFixedThreadPool(maxUploadThreads);
        recoveryPool = Executors.newFixedThreadPool(maxRecoveryThreads);
        Configuration conf = new Configuration();
        if (conf.get("io.compression.codecs") == null) {
            conf.set("io.compression.codecs",
                    "org.apache.hadoop.io.compress.GzipCodec,"
                    + "org.apache.hadoop.io.compress.DefaultCodec,"
                    + "com.hadoop.compression.lzo.LzoCodec,"
                    + "com.hadoop.compression.lzo.LzopCodec,"
                    + "org.apache.hadoop.io.compress.BZip2Codec,"
                    + "org.apache.hadoop.io.compress.SnappyCodec");
        }
        if (conf.get("io.compression.codec.lzo.class") == null) {
            conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
        }
        fs = (new Path(hdfsbase)).getFileSystem(conf);
        rolls = new ConcurrentHashMap<RollIdent, RollPartition>();
        accepter = new RecoveryAcceptor(recoverySocketTimeout);
        accepter.start();
        LOG.info("roll manager started");
    }
    
    public boolean perpareUpload(String app, String source, long period, RollPartition rollPartition) {
        boolean ret;
        RollIdent ident = getRollIdent(app, source, period);
        if (rolls.get(ident) == null) {
            rolls.put(ident, rollPartition);
            Message message = PBwrap.wrapReadyUpload(ident.topic, ident.source, ident.period, ident.ts);
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
            Message message = PBwrap.wrapRollClean(ident.topic, ident.source, ident.period);
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
        ident.source = rollID.getSource();
        ident.period = rollID.getPeriod();
        ident.ts = rollID.getRollTs();
        ident.isFinal = rollID.getIsFinal();
        ident.persistent = rollID.getPersistent();
        
        RollPartition roll = rolls.get(ident);
        
        if (roll == null) {
            LOG.error("can not find roll by rollident " + ident);
            reportUpload(ident, rollID.getCompression(), false);
            return false;
        }
        
        LOG.info("start to upload roll " + ident);
        StorageManager manager = Broker.getBrokerService().getPersistentManager();
        HDFSUpload upload = new HDFSUpload(this, manager, fs, ident, roll, rollID.getCompression());
        uploadPool.execute(upload);
        //start a ftp uploader which need uploading to work with HDFS in parallel.
        //ftp uploader use gz compression algo by default
        FTPConfigration ftpConf;
        if ((ftpConf = FTPConfigrationLoader.getFTPConfigration(ident.topic)) != null) {
            Partition p = manager.getPartition(ident.topic, ident.source);
            LOG.info("start to ftp " + ident);
            FTPUpload ftpUpload = new FTPUpload(this, ftpConf, ident, roll, p);
            Thread ftpThread = new Thread(ftpUpload);
            ftpThread.start();
        }
        return true;
    }

    public void markUnrecoverable(RollID rollID) {
        RollIdent ident = new RollIdent();
        ident.topic = rollID.getTopic();
        ident.source = rollID.getSource();
        ident.period = rollID.getPeriod();
        ident.ts = rollID.getRollTs();
        
        LOG.info("start to mark unrecoverable roll " + ident);
        HDFSMarker marker = new HDFSMarker(this, fs, ident);
        uploadPool.execute(marker);
    }
    
    private RollIdent getRollIdent(String app, String source, long period) {
        Date time = new Date(Util.getLatestRollTsUnderTimeBuf(Util.getTS(), period, clockSyncBufMillis));
        RollIdent roll = new RollIdent();
        roll.topic = app;
        roll.source = source;
        roll.period = period;
        roll.ts = time.getTime();
        return roll;
    }
    
    private RollIdent getLastRollIdent(String app, String source, long period) {
        Date time = new Date(Util.getCurrentRollTs(Util.getTS(), period));
        RollIdent roll = new RollIdent();
        roll.topic = app;
        roll.source = source;
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
        format = Util.getFormatFromPeriodForPath(ident.period);
        Date roll = new Date(ident.ts);
        SimpleDateFormat dm = new SimpleDateFormat(format);
        return baseDir + "/" + ident.topic + '/' + getDatepathbyFormat(dm.format(roll));
    }
    
    /*
     * machine01@appname_2013-11-01.14.08
     */
    public String getFileName(RollIdent ident) {
        String format;
        format = Util.getFormatFromPeriod(ident.period);
        Date roll = new Date(ident.ts);
        SimpleDateFormat dm = new SimpleDateFormat(format);
        return ident.source + '@' + ident.topic + "_" + dm.format(roll);
    }
    
    private String getHiddenFileName(RollIdent ident) {
        return "_" + getFileName(ident);
    }
    
    public String getCompressedFileName(RollIdent ident) {
        return getCompressedFileName(ident, this.defaultCompression);
    }
    
    public String getGZCompressedFileName(RollIdent ident) {
        return getCompressedFileName(ident, ParamsKey.COMPRESSION_GZ);
    }
    
    public String getCompressedFileName(RollIdent ident, String compressionAlgoName) {
        return getFileName(ident) + "." + compressionAlgoName;
    }

    /*
     * Path format:
     * hdfsbasedir/appname/2013-11-01/14/08/machine01@appname_2013-11-01.14.08.gz(lzo)
     */
    public String getRollHdfsPath(RollIdent ident) {
        return getParentPath(hdfsbase, ident) + getCompressedFileName(ident);
    }
    
    public String getRollHdfsPath(RollIdent ident, String compressionAlgoName) {
        return getParentPath(hdfsbase, ident) + getCompressedFileName(ident, compressionAlgoName);
    }
    
    public String getMarkHdfsPath(RollIdent ident) {
        return getParentPath(hdfsbase, ident) + getHiddenFileName(ident);
    }
    
    public String getTempHdfsPath(RollIdent ident) {
        return getParentPath(hdfsbase, ident) + getFileName(ident) + TMP_SUFFIX;
    }

    public String getRecoveryHdfsPath(RollIdent ident) {
        return getParentPath(hdfsbase, ident) + getFileName(ident) + R_SUFFIX;
    }
    
    public void reportRecovery(RollIdent ident, boolean recoverySuccess) {
        Message message;
        if (recoverySuccess) {
            message = PBwrap.wrapRecoverySuccess(ident.topic, ident.source, ident.period, ident.ts, ident.isFinal, ident.persistent);
        } else {
            message = PBwrap.wrapRecoveryFail(ident.topic, ident.source, ident.period, ident.ts, ident.isFinal);
        }
        Broker.getSupervisor().send(message);
    }

    public void reportUpload(RollIdent ident, String compression, boolean uploadSuccess) {
        rolls.remove(ident);
        
        if (uploadSuccess) {
            Message message = PBwrap.wrapUploadSuccess(ident.topic, ident.source, ident.period, ident.ts, ident.isFinal, ident.persistent, compression);
            Broker.getSupervisor().send(message);
        } else {
            Message message = PBwrap.wrapUploadFail(ident.topic, ident.source, ident.period, ident.ts, ident.isFinal, compression);
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
            this.server = new ServerSocket(port);
            this.setName("RecoveryAcceptor");
            this.setDaemon(true);
        }
        
        public void close() throws IOException {
            server.close();
        }
        
        @Override
        public void run() {
            while (running) {
                try {
                    Socket socket = server.accept();
                    LOG.debug("Socket(Recovery) connected from " + socket.getInetAddress().getHostName());
                    socket.setSoTimeout(recoverySocketTimeout);
                    
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    
                    AgentProtocol protocol = new AgentProtocol();
                    AgentHead head = protocol.new AgentHead();
                    
                    protocol.recieveHead(in, head);

                    RollIdent roll = new RollIdent();
                    roll.topic = head.app;
                    // be compatible with old version
                    if (head.version == AgentProtocol.VERSION_MICOR_BATCH) {
                        roll.source = head.source;
                    } else {
                        roll.source = Util.getRemoteHost(socket);
                        if (head.source != null) {
                            roll.source += "#" + head.source;
                        }
                    }
                    roll.period = head.period;
                    roll.ts = head.ts;
                    roll.isFinal = head.isFinal;
                    roll.persistent = head.isPersist;
                    
                    if (head.ignore) {
                        LOG.info("ignore and mark unrecoverable roll " + roll);
                        HDFSMarker marker = new HDFSMarker(RollManager.this, fs, roll);
                        uploadPool.execute(marker);
                        reportRecovery(roll, true);
                    } else {
                        LOG.info("start to recovery roll " + roll);
                        HDFSRecovery recovery = new HDFSRecovery(
                                RollManager.this, fs, socket, roll, head.size, head.hasCompressed);
                        recoveryPool.execute(recovery);
                    }
                } catch (IOException e) {
                    LOG.error("error in acceptor: ", e);
                }
            }
        }
    }
}

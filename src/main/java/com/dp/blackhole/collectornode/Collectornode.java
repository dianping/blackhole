package com.dp.blackhole.collectornode;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.RollIDPB.RollID;
import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.node.Node;

public class Collectornode extends Node {

    private ExecutorService pool;
    private ServerSocket server;
    private ConcurrentHashMap<RollIdent, File> fileRolls;
    private String basedir;
    private String hdfsbasedir;
    private String suffix;
    private int port;
    private FileSystem fs;

    public Collectornode() throws IOException {
        pool = Executors.newCachedThreadPool();
        fileRolls = new ConcurrentHashMap<RollIdent, File>();
    }

    private class Acceptor extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    Socket socket = server.accept();
                    
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    
                    AgentProtocol protocol = new AgentProtocol();
                    AgentHead head = protocol.new AgentHead();
                    
                    protocol.recieveHead(in, head);
                                 
                    if (AgentProtocol.STREAM == head.type) {
                        LOG.debug("logreader connected: " + head.app);
                        Collector collector = new Collector(Collectornode.this, socket, basedir, head.app, getHost(), head.peroid);
                        pool.execute(collector);
                        Message msg = PBwrap.wrapReadyCollector(head.app, getHost(), head.peroid, getHost(), Util.getTS());
                        send(msg);
                    } else if (AgentProtocol.RECOVERY == head.type) {
                        
                        RollIdent roll = new RollIdent();
                        roll.app = head.app;
                        roll.source = getHost();
                        roll.period = head.peroid;
                        roll.ts = head.ts;
                        
                        HDFSRecovery recovery = new HDFSRecovery(Collectornode.this, fs, socket, roll);
                        pool.execute(recovery);
                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
    
    @Override
    protected boolean process(Message msg) {
        boolean ret = false;
        LOG.debug("process message: " + msg);
        switch (msg.getType()) {
        case UPLOAD_ROLL:
            ret = uploadRoll(msg.getRollID());
        default:
        }
        return ret;
    }

    private boolean uploadRoll(RollID rollID) {
        RollIdent ident = new RollIdent();
        ident.app = rollID.getAppName();
        ident.source = rollID.getAppServer();
        ident.period = rollID.getPeriod();
        ident.ts = rollID.getRollTs();
        
        File roll = fileRolls.get(ident);
        
        HDFSUpload upload = new HDFSUpload(this, fs, roll, ident);
        pool.execute(upload);
        return true;
    }

    public String getDatepathbyFormat (String format) {
        StringBuffer dirs = new StringBuffer();
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
    public String getRollHdfsPathPrefix (RollIdent ident) {
        String format;
        format = Util.getFormatFromPeroid(ident.period);
        Date roll = new Date(ident.ts);
        SimpleDateFormat dm= new SimpleDateFormat(format);
        return hdfsbasedir + '/' + ident.app + '/' + getDatepathbyFormat(dm.format(roll)) + 
                ident.source + '@' + ident.app + "_" + dm.format(roll);
    }
    
    public String getRollHdfsPath (RollIdent ident) {
        return getRollHdfsPathPrefix(ident) + suffix;
    }
    
    private void start() throws FileNotFoundException, IOException {        
        Properties prop = new Properties();
        prop.load(new FileReader(new File("config.properties")));
        
        basedir = prop.getProperty("collectornode.basedir");
        port = Integer.parseInt(prop.getProperty("collectornode.port"));
        hdfsbasedir = prop.getProperty("hdfs.basedir");
        suffix = prop.getProperty("hdfs.file.suffix");
        
        if (basedir == null || hdfsbasedir == null || suffix == null) {
            throw new IOException("config in config.properties missed");
        }
        
        server = new ServerSocket(port);
        fs = (new Path(hdfsbasedir)).getFileSystem(new Configuration());
        
        cleanupLocalStorage();
        
        // start to accept connection
        Acceptor acceptor = new Acceptor();
        acceptor.setDaemon(true);
        acceptor.start();
        
        registerNode();
        
        loop();
        close();
    }
    
    private void delete(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            for (File f : children) {
                delete(f);
            }
            file.delete();
        } else {
            file.delete();
        }
    }
    
    private void cleanupLocalStorage() {
        File base = new File(basedir);
        if (base.exists()) {
            delete(base);
        }
        base.mkdir();
    }

    private void registerNode() {  
        send(PBwrap.wrapCollectorReg());
        LOG.info("register collector node with supervisor");
    }

    private void close() {
        LOG.info("shutdown collector node");
        pool.shutdownNow();
        try {
            server.close();
        } catch (IOException e) {
            LOG.error("error close ServerSocket" + e);
        }
    }

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        try {
        Collectornode node = new Collectornode();
        node.start();
        } catch (Throwable e) {
            LOG.error("fatal error " + e);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public File getappendingFile(String storagedir) {
        File parent = new File(storagedir);
        parent.mkdirs();
        File file = new File(parent, "appending." + Util.getTS() + ".gz");
        return file;
    }

    public void registerfile(RollIdent rollIdent, File rollFile) {
        if (fileRolls.get(rollIdent) == null) {
            fileRolls.put(rollIdent, rollFile);
            Message message = PBwrap.wrapAppRoll(rollIdent.app, rollIdent.source, rollIdent.period, rollIdent.ts);
            send(message);
        } else {
            LOG.fatal("update a exists file roll");
        }
    }
    
    public String getSuffix() {
        return suffix;
    }
    
    public void recoveryResult(RollIdent ident, boolean recoverySuccess) {
        Message message;
        if (recoverySuccess == true) {
            message = PBwrap.wrapRecoverySuccess(ident.app, ident.source, ident.period, ident.ts);
        } else {
            message = PBwrap.wrapRecoveryFail(ident.app, ident.source, ident.period, ident.ts);
        }
        send(message);
    }

    public void uploadResult(RollIdent ident, boolean uploadSuccess) {        
        if (uploadSuccess == true) {
            Message message = PBwrap.wrapUploadSuccess(ident.app, ident.source, ident.period, ident.ts);
            send(message);
            File f = fileRolls.get(ident);
            if (!f.delete()) {
                LOG.error("delete file " + f + " failed");
            }
        } else {
            Message message = PBwrap.wrapUploadFail(ident.app, ident.source, ident.period, ident.ts);
            send(message);
        }
    }
}
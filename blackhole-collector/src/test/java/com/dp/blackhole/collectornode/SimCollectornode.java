package com.dp.blackhole.collectornode;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.conf.ConfigKeeper;

import static org.junit.Assert.*;

public class SimCollectornode extends Collectornode implements Runnable{
    private static final Log LOG = LogFactory.getLog(SimCollectornode.class);
    public static final String HOSTNAME = "localhost";
    public static long rollTS = 1357023691855l;
    public static final String SCHEMA = "file://";
    public static final String BASE_PATH = "/tmp/hdfs/";
    public static final String BASE_HDFS_PATH = SCHEMA + BASE_PATH;
    public static final String FILE_SUFFIX = "2013-01-01.15";
    public static final String expected = " 0f j2390jr092jf2f02jf02qjdf2-3j0 fiopwqejfjwffhg5_p    <end";
    private FileSystem fs;
    private String simType;
    private ServerSocket ss;
    private String appName;
    private int port;
    private Socket client;
    private List<String> receives;
    private volatile boolean shouldStop;
    
    /**
     * not need port
     * @param simType
     * @param appName
     * @param fs
     * @throws IOException
     */
    private SimCollectornode(String simType, String appName, FileSystem fs) throws IOException {
        this.simType = simType;
        this.appName = appName;
        this.fs = fs;
    }

    /**
     * not need receive list
     * @param simType
     * @param appName
     * @param port
     * @param fs
     * @throws IOException
     */
    public SimCollectornode(String simType, String appName, int port, FileSystem fs) throws IOException {
        this(simType, appName, port, fs, null);
    }
    
    /**
     * not need to assign receive list
     * @param simType
     * @param appName
     * @param port
     * @throws IOException
     */
    public SimCollectornode(String simType, String appName, int port) throws IOException
    {
        this(simType, appName, port, null, new ArrayList<String>());
    }
    
    /**
     * not need FS
     * @param simType
     * @param appName
     * @param port
     * @param receives
     * @throws IOException
     */
    public SimCollectornode(String simType, String appName, int port, List<String> receives) throws IOException {
        this(simType, appName, port, null, receives);
    }
    
    public SimCollectornode(String simType, String appName, int port, FileSystem fs, List<String> receives) throws IOException {
        this.simType = simType;
        this.appName = appName;
        this.fs = fs;
        this.receives = receives;
        this.port = port;
        ss = new ServerSocket(port);
        this.shouldStop = false;
    }
    
    public boolean shouldStopOrNot() {
        return shouldStop;
    }
    
    public void stopIt() {
        shouldStop = true;
        try {
            if (!ss.isClosed()) {
                ss.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static SimCollectornode getSimpleInstance(String simType, String appName, FileSystem fs) throws IOException {
        return new SimCollectornode(simType, appName, fs);
    }
    
    @Override
    public void recoveryResult(RollIdent ident, boolean recoverySuccess) {
        LOG.debug("send recovery result: " + recoverySuccess);
    }
    @Override
    public void uploadResult(RollIdent ident, boolean uploadSuccess) {
        LOG.debug("send upload result: " + uploadSuccess);
    }
    @Override
    public String getRollHdfsPath (RollIdent ident) {
        String format;
        format = Util.getFormatFromPeroid(ident.period);
        Date roll = new Date(ident.ts);
        SimpleDateFormat dm= new SimpleDateFormat(format);
        return BASE_HDFS_PATH + ident.app + '/' + getDatepathbyFormat(dm.format(roll)) + 
                ident.source + '@' + ident.app + "_" + dm.format(roll) + ".gz";
    }
    @Override
    public void run() {
        LOG.debug("server begin at " + port);
        try {
            while (!Thread.interrupted()) {
                String line = null;
                client = ss.accept();
                DataInputStream din = new DataInputStream(client.getInputStream());
                if (simType.equals("recovery")) {
                    AgentProtocol protocol = new AgentProtocol();
                    AgentHead head = protocol.new AgentHead();
                    protocol.recieveHead(din, head);
                    String appname = head.app;
                    LOG.debug("Receive... " + appname);
                    assertEquals(appName, head.app);
                    long period = ConfigKeeper.configMap.get(appName)
                            .getLong(ParamsKey.Appconf.ROLL_PERIOD, 3600l);
                    LOG.debug("Receive... " + period);
                    assertEquals(period, head.peroid);
                    assertEquals(rollTS, head.ts);
                    RollIdent roll = new RollIdent();
                    roll.app = head.app;
                    roll.source = HOSTNAME;
                    roll.period = head.peroid;
                    roll.ts = head.ts;
                    HDFSRecovery recovery = new HDFSRecovery(getSimpleInstance(simType, appName, fs), 
                            fs, client, roll);
                    recovery.run();
                } else if (simType.equals("stream")) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(din));
                    while (!Thread.interrupted() && !shouldStop && (line = reader.readLine()) != null) {
                        LOG.debug("server>" + line);
                        receives.add(line);
                    }
                    if (din != null) {
                        din.close();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (!ss.isClosed()) {
                    ss.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
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
}

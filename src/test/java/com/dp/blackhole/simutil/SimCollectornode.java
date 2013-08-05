package com.dp.blackhole.simutil;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

import com.dp.blackhole.collectornode.Collectornode;
import com.dp.blackhole.collectornode.HDFSRecovery;
import com.dp.blackhole.collectornode.RollIdent;
import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.conf.ConfigKeeper;

import static org.junit.Assert.*;

public class SimCollectornode extends Collectornode implements Runnable{
    private static final Log LOG = LogFactory.getLog(SimCollectornode.class);
    private FileSystem fs;
    private String simType;
    private ServerSocket ss;
    private String appName;
    private Socket client;
    
    private SimCollectornode(String simType, FileSystem fs, String appName) throws IOException {
        this.fs = fs;
        this.simType = simType;
        this.appName = appName;
    }

    public SimCollectornode(String simType, int port, FileSystem fs, String appName) throws IOException {
        this.fs = fs;
        this.simType = simType;
        this.appName = appName;
        ss = new ServerSocket(port);
    }
    
    public static SimCollectornode getSimpleInstance(String simType, FileSystem fs, String appName) throws IOException {
        return new SimCollectornode(simType, fs, appName);
    }
    
    @Override
    public void recoveryResult(RollIdent ident, boolean recoverySuccess) {
        LOG.info("send recovery result: " + recoverySuccess);
    }
    @Override
    public void uploadResult(RollIdent ident, boolean uploadSuccess) {
        LOG.info("send upload result: " + uploadSuccess);
    }
    @Override
    public String getRollHdfsPath (RollIdent ident) {
        String format;
        format = Util.getFormatFromPeroid(ident.period);
        Date roll = new Date(ident.ts);
        SimpleDateFormat dm= new SimpleDateFormat(format);
        return com.dp.blackhole.simutil.Util.BASE_HDFS_PATH + ident.app + '/' + getDatepathbyFormat(dm.format(roll)) + 
                ident.source + '@' + ident.app + "_" + dm.format(roll) + ".gz";
    }
    @Override
    public void run() {
        try {
            client = ss.accept();
            if (simType.equals("recovery")) {
                DataInputStream din = new DataInputStream(client.getInputStream());
                AgentProtocol protocol = new AgentProtocol();
                AgentHead head = protocol.new AgentHead();
                
                protocol.recieveHead(din, head);
                assertEquals(appName, head.app);
                long period = ConfigKeeper.configMap.get(appName)
                        .getLong(ParamsKey.Appconf.ROLL_PERIOD, 3600l);
                assertEquals(period, head.peroid);
                assertEquals(com.dp.blackhole.simutil.Util.rollTS, head.ts);
                RollIdent roll = new RollIdent();
                roll.app = head.app;
                roll.source = com.dp.blackhole.simutil.Util.HOSTNAME;
                roll.period = head.peroid;
                roll.ts = head.ts;
                HDFSRecovery recovery = new HDFSRecovery(getSimpleInstance(simType, fs, appName), 
                        fs, client, roll);
                recovery.run();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
    }

    
}

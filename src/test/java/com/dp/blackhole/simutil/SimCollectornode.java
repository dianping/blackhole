package com.dp.blackhole.simutil;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

import com.dp.blackhole.collectornode.Collectornode;
import com.dp.blackhole.collectornode.HDFSRecovery;
import com.dp.blackhole.collectornode.HDFSUpload;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.AppConfigurationConstants;
import com.dp.blackhole.conf.ConfigKeeper;

import static org.junit.Assert.*;

public class SimCollectornode extends Collectornode implements Runnable{
    private static final Log LOG = LogFactory.getLog(SimCollectornode.class);
    private FileSystem fs;
    private String simType;
    private ServerSocket ss;
    private String appName;
    private String appHost;
    private String fileSuffix;
    private long position;
    private long length;
    private Socket client;
    
    private SimCollectornode(String simType, FileSystem fs, String appName) throws IOException {
        this.fs = fs;
        this.simType = simType;
        this.appName = appName;
        this.appHost = "localhost";
        this.fileSuffix = com.dp.blackhole.simutil.Util.FILE_SUFFIX;
        this.position = 0;
        this.length = 0;
    }
    
    public static SimCollectornode getSimpleInstance(String simType, FileSystem fs, String appName) throws IOException {
        return new SimCollectornode(simType, fs, appName);
    }
    
    public SimCollectornode(String simType, int port, FileSystem fs, String appName, String appHost, 
            String fileSuffix, long position, long length) throws IOException {
        this.fs = fs;
        this.simType = simType;
        this.appName = appName;
        this.appHost = appHost;
        this.fileSuffix = fileSuffix;
        this.position = position;
        this.length = length;
        ss = new ServerSocket(port);
    }
    
    @Override
    public void recoveryResult(HDFSRecovery hdfsRecovery,
            boolean recoverySuccess) {
        LOG.info("send recovery result: " + recoverySuccess);
    }
    @Override
    public void uploadResult(HDFSUpload hdfsUpload, boolean uploadSuccess) {
        LOG.info("send upload result: " + uploadSuccess);
    }
    
    @Override
    public void run() {
        try {
            client = ss.accept();
            if (simType.equals("recovery")) {
                DataInputStream din = new DataInputStream(client.getInputStream());
                assertEquals(simType, Util.readString(din));
                assertEquals(appName, Util.readString(din));
                String unit = ConfigKeeper.configMap.get(appName)
                        .getString(AppConfigurationConstants.TRANSFER_PERIOD_UNIT, "hour");
                int value = ConfigKeeper.configMap.get(appName)
                        .getInteger(AppConfigurationConstants.TRANSFER_PERIOD_VALUE, 1);
                long period = Util.getPeriodInSeconds(value, unit);
                assertEquals(period, din.readLong());
                assertEquals(Util.getFormatByUnit(unit), Util.readString(din));
                HDFSRecovery recovery = new HDFSRecovery(getSimpleInstance(simType, fs, appName), fs, client, appName, appHost, 
                        fileSuffix, position, length);
                recovery.run();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

    
}

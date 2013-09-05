package com.dp.blackhole.collectornode;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dp.blackhole.appnode.AppLog;
import com.dp.blackhole.appnode.RollRecovery;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.simutil.SimAppnode;
import com.dp.blackhole.simutil.SimCollectornode;
import com.dp.blackhole.simutil.Util;


public class TestHDFSRecovery {
    private static final Log LOG = LogFactory.getLog(TestHDFSRecovery.class);
    private final String MAGIC = "e9wjd83h";
    private final String APP_HOST = "localhost";
    private File file;
    private File fileBroken;
    private FileSystem fs;
    private Path oldPath;
    private SimAppnode appnode;
    private String client;

    @Before
    public void setUp() throws Exception {
        //build a tmp file
        fileBroken = Util.createBrokenTmpFile(MAGIC + "_broken_", Util.expected);
        Util.convertToGZIP(fileBroken);
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            LOG.error("Failed to get FileSystem.");
            e.printStackTrace();
            throw e;
        }
        //file:///tmp/e9wjd83h/2013-01-01/15/localhost_e9wjd83h_2013-01-01.03  e9wjd83h is appname
        oldPath = new Path(Util.SCHEMA + Util.BASE_PATH 
                + MAGIC + "/2013-01-01/15/" + APP_HOST + "@"
                + MAGIC + "_2013-01-01.15.gz.tmp");
        LOG.info("old path in hdfs is " + oldPath);
        fs.copyFromLocalFile(false, true, new Path(fileBroken.toURI()), oldPath);
        
        //create a good file and rename it for client
        file = Util.createTmpFile(MAGIC, Util.expected);
        Util.createTmpFile(MAGIC + "." + Util.FILE_SUFFIX, Util.expected);
        Util.createTmpFile(APP_HOST + "@" + MAGIC + "_" + Util.FILE_SUFFIX, Util.expected);
        
        try {
            client = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            LOG.error("Oops, got an exception:", e1);
            return;
        }
        appnode = new SimAppnode(client);
        
        try {
            client = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            LOG.error("Oops, got an exception:", e1);
            return;
        }
        appnode = new SimAppnode(client);
        
        //deploy some condition
        ConfigKeeper confKeeper = new ConfigKeeper();
        confKeeper.addRawProperty(MAGIC+".ROLL_PERIOD", "3600");
    }

    @After
    public void tearDown() throws Exception {
        fs.delete(new Path(Util.SCHEMA + Util.BASE_PATH), true);
        fileBroken.delete();
        file.delete();
        Util.deleteTmpFile(MAGIC);
    }

    @Test
    public void test() throws IOException, InterruptedException {
        Thread serverThread = new Thread(
                new SimCollectornode("recovery", Util.PORT, fs, MAGIC));
        serverThread.start();
        
        AppLog appLog = new AppLog(MAGIC, file.getAbsolutePath(), new Date().getTime());
        RollRecovery clientTask = new RollRecovery(appnode, Util.HOSTNAME, Util.PORT, appLog, Util.rollTS);
        Thread clientThread = new Thread(clientTask);
        clientThread.start();
        
        serverThread.join();
        clientThread.join();
        Util.convertToGZIP(file);
        String expectedMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(file));
        
//        LOG.info("expected md5 is " + expectedMD5);
        Path newPath = new Path(Util.SCHEMA + Util.BASE_PATH 
                + MAGIC + "/2013-01-01/15/" + APP_HOST + "@"
                + MAGIC + "_2013-01-01.15.gz");
        File fileGzip = new File(newPath.toUri());
//        fileGzip = Util.convertToNomal(fileGzip);
        String actualMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(fileGzip));
//        LOG.info("actual md5 is " + actualMD5);
        assertEquals("recovery file is not correct.", expectedMD5, actualMD5);
    }

}

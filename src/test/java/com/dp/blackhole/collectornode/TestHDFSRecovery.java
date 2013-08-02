package com.dp.blackhole.collectornode;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.appnode.AppLog;
import com.dp.blackhole.appnode.RollRecovery;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.simutil.SimCollectornode;
import com.dp.blackhole.simutil.Util;


public class TestHDFSRecovery {
    private static final Log LOG = LogFactory.getLog(TestHDFSRecovery.class);
    private static final String MAGIC = "e9wjd83h";
    private static final String APP_HOST = "localhost";
    private static File file;
    private static File fileBroken;
    private static File fileBrokenGzip;
    private static FileSystem fs;
    private static Path oldPath;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        //build a tmp file
        fileBroken = Util.createBrokenTmpFile(MAGIC + "_broken_", Util.expected);
        fileBrokenGzip = Util.convertToGZIP(fileBroken);
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            LOG.error("Failed to get FileSystem.");
            e.printStackTrace();
            throw e;
        }
        //file:///tmp/e9wjd83h/2013-01-01/03/localhost_e9wjd83h_2013-01-01.03  e9wjd83h is appname
        oldPath = new Path(Util.SCHEMA + Util.BASE_PATH 
                + MAGIC + "/2013-01-01/03/" + APP_HOST + "_"
                + MAGIC + "_2013-01-01.03.tmp");
        LOG.info("old path in hdfs is " + oldPath);
        fs.copyFromLocalFile(false, true, new Path(fileBrokenGzip.toURI()), oldPath);
        
        //create a good file and rename it for client
        file = Util.createTmpFile(MAGIC, Util.expected);
        File renameFile = new File(file.getParent(), APP_HOST + "_" + MAGIC + "_" + Util.FILE_SUFFIX);
        file.renameTo(renameFile);
        file = renameFile;
        
        //deploy some condition
        ConfigKeeper confKeeper = new ConfigKeeper();
        confKeeper.addRawProperty(MAGIC+".TRANSFER_PERIOD_VALUE", "1");
        confKeeper.addRawProperty(MAGIC+".TRANSFER_PERIOD_UNIT", "hour");
        //base_hdfs_path
        confKeeper.addRawProperty(MAGIC+".port", "40000");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        fs.delete(new Path(Util.SCHEMA + Util.BASE_PATH + MAGIC), true);
        fileBrokenGzip.delete();
        file.delete();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() throws IOException, InterruptedException {
        Thread serverThread = new Thread(
                new SimCollectornode("recovery", Util.PORT, fs, MAGIC, APP_HOST, 0, 0));
        serverThread.start();
        
        AppLog appLog = new AppLog(MAGIC, file.getAbsolutePath(), new Date().getTime());
        RollRecovery clientTask = new RollRecovery(Util.HOSTNAME, Util.PORT, appLog, Util.rollTS);
        Thread clientThread = new Thread(clientTask);
        clientThread.start();
        
        serverThread.join();
        clientThread.join();
        file = Util.convertToGZIP(file);
        String expectedMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(file));
        
//        LOG.info("expected md5 is " + expectedMD5);
        Path newPath = new Path(Util.SCHEMA + Util.BASE_PATH 
                + MAGIC + "/2013-01-01/03/" + APP_HOST + "_"
                + MAGIC + "_2013-01-01.03");
        File fileGzip = new File(newPath.toUri());
//        fileGzip = Util.convertToNomal(fileGzip);
        String actualMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(fileGzip));
//        LOG.info("actual md5 is " + actualMD5);
        assertEquals("recovery file is not correct.", expectedMD5, actualMD5);
    }

}

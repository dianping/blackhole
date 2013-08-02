package com.dp.blackhole.appnode;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.simutil.SimRecoveryServer;
import com.dp.blackhole.simutil.Util;

public class TestRollRecovery {
    private static final Log LOG = LogFactory.getLog(TestRollRecovery.class);
    private static final String MAGIC = "ctg4ewd";
    private static File file;
    private static AppLog appLog;
    private static List<String> header = new ArrayList<String>();
    private static List<String> receives = new ArrayList<String>();
    private SimRecoveryServer server;
    private Thread serverThread;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        ConfigKeeper confKeeper = new ConfigKeeper();
        confKeeper.addRawProperty(MAGIC+".port", "40000");
        confKeeper.addRawProperty(MAGIC+".transferPeriodValue", "1");
        confKeeper.addRawProperty(MAGIC+".transferPeriodUnit", "hour");
        //build a tmp file
        file = Util.createTmpFile(MAGIC + Util.FILE_SUFFIX, Util.expected);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Util.deleteTmpFile(MAGIC);
    }

    @Before
    public void setUp() throws Exception {
        appLog = new AppLog(MAGIC, file.getAbsolutePath(), System.currentTimeMillis());
        server = new SimRecoveryServer(Util.PORT, header, receives);
        serverThread = new Thread(server);
        serverThread.start();

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {
        RollRecovery recovery = new RollRecovery(Util.HOSTNAME, Util.PORT, appLog, Util.rollTS);
        Thread thread = new Thread(recovery);
        thread.start();
        try {
            serverThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String[] expectedHeader = new String[4];
        expectedHeader[0] = "recovery";
        expectedHeader[1] = MAGIC;
        expectedHeader[2] = "3600";
        expectedHeader[3] = "yyyy-MM-dd.hh";
        assertArrayEquals("head not match", expectedHeader, header.toArray());
        assertEquals("loader function fail.", Util.expected, receives.get(receives.size()-1));
    }
}

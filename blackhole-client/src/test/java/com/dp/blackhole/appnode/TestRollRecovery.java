package com.dp.blackhole.appnode;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.appnode.AppLog;
import com.dp.blackhole.appnode.RollRecovery;
import com.dp.blackhole.conf.ConfigKeeper;

public class TestRollRecovery {
    private static final String MAGIC = "ctg4ewd";
    private static final int port = 40002;
    private static File file;
    private static AppLog appLog;
    private static List<String> header = new ArrayList<String>();
    private static List<String> receives = new ArrayList<String>();
    private Thread serverThread;
    private static SimAppnode appnode;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        appnode = new SimAppnode();
        ConfigKeeper confKeeper = new ConfigKeeper();
        confKeeper.addRawProperty(MAGIC + ".rollPeriod", "3600");
        confKeeper.addRawProperty(MAGIC + ".maxLineSize", "1024");
        //build a tmp file
        file = SimAppnode.createTmpFile(MAGIC + "." + SimAppnode.FILE_SUFFIX, SimAppnode.expected);
        file = SimAppnode.createTmpFile(MAGIC, SimAppnode.expected);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        SimAppnode.deleteTmpFile(MAGIC);
    }

    @Before
    public void setUp() throws Exception {
        appLog = new AppLog(MAGIC, file.getAbsolutePath(), System.currentTimeMillis(), 1024);
        SimRecoveryServer server = new SimRecoveryServer(port, header, receives);
        serverThread = new Thread(server);
        serverThread.start();
    }

    @After
    public void tearDown() throws Exception {
//        serverThread.interrupt();
    }

    @Test
    public void test() {
        RollRecovery recovery = new RollRecovery(appnode, SimAppnode.HOSTNAME, port, appLog, SimAppnode.rollTS);
        Thread thread = new Thread(recovery);
        thread.start();
        try {
            serverThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String[] expectedHeader = new String[4];
        expectedHeader[0] = "2";
        expectedHeader[1] = MAGIC;
        expectedHeader[2] = "3600";
        expectedHeader[3] = String.valueOf(SimAppnode.rollTS);
        assertArrayEquals("head not match", expectedHeader, header.toArray());
        assertEquals("loader function fail.", SimAppnode.expected, receives.get(receives.size()-1));
    }
}

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

import com.dp.blackhole.conf.Configuration;
import com.dp.blackhole.testutil.RecoveryServer;
import com.dp.blackhole.testutil.Util;

public class TestRollRecoveryZero {
    private static final Log LOG = LogFactory.getLog(TestRollRecoveryZero.class);
    private static final String MAGIC = "fee54we";
    private static final String rollIdent = "2013-01-01.15:00:03";
    private static File file;
    private static final int PORT = 40000;
    private static AppLog appLog;
    private static List<String> header = new ArrayList<String>();
    private static List<String> receives = new ArrayList<String>();
    private RecoveryServer server;
    private Thread serverThread;
    private static final String expected = " 0f j2390jr092jf2f02jf02qjdf2-3j0 fiopwqejfjwffhg5_p    <end";
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    //build a tmp file
        file = Util.createTmpFile(MAGIC + rollIdent, expected);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        LOG.info("delete tmp file for test RollRecoveryZero " + file);
        Util.deleteTmpFile(MAGIC);
    }

    @Before
    public void setUp() throws Exception {
        appLog = new AppLog(MAGIC, file.getAbsolutePath(), System.currentTimeMillis(), "localhost", PORT);
        server = new RecoveryServer(PORT, header, receives);
        serverThread = new Thread(server);
        serverThread.start();
        Configuration conf = new Configuration();
        conf.addRawProperty(MAGIC+".transferPeriodValue", "1");
        conf.addRawProperty(MAGIC+".transferPeriodUnit", "hour");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {
        RollRecoveryZero recoveryZ = new RollRecoveryZero(appLog, rollIdent);
        Thread thread = new Thread(recoveryZ);
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
        assertEquals("loader function fail.", expected, receives.get(receives.size()-1));
    }
}

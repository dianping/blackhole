package com.dp.blackhole.appnode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.conf.ConfigKeeper;

public class TestAppnode {
    private static final String MAGIC = "9vjrder3";
    private SimAppnode appnode;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        ConfigKeeper.configMap.clear();
        ConfigKeeper conf = new ConfigKeeper();
        conf.addRawProperty(MAGIC+"."+ParamsKey.Appconf.WATCH_FILE, "/tmp/" + MAGIC + ".log");
        conf.addRawProperty(MAGIC+"."+ParamsKey.Appconf.ROLL_PERIOD, "3600");
        conf.addRawProperty(MAGIC+"."+ParamsKey.Appconf.MAX_LINE_SIZE, "1024");
        File tailFile = new File("/tmp/" + MAGIC + ".log");
        tailFile.createNewFile();
        tailFile.deleteOnExit();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        appnode = new SimAppnode();
        appnode.fillUpAppLogsFromConfig();
    }

    @After
    public void tearDown() throws Exception {
//        appnode.onDisconnected();
    }

    /**
     * specifiedFileName: "/tmp/hostna.access.log"
     * expectedFileName: "/tmp/hostname.nh.access.log"
     */
    @Test
    public void testCheckAllFilesExist() throws IOException {
        String specifiedFileName = "/tmp/" + InetAddress.getLocalHost().getHostName().substring(0, 2) + ".access.log";
        String expectedFileName = "/tmp/" + InetAddress.getLocalHost().getHostName() + ".access.log";
        File expectedFile = new File(expectedFileName);
        expectedFile.createNewFile();
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.Appconf.WATCH_FILE, "/tmp/" + MAGIC + ".log");
        assertTrue(appnode.checkAllFilesExist());
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.Appconf.WATCH_FILE, specifiedFileName);
        assertTrue(appnode.checkAllFilesExist());
        assertEquals(expectedFileName, ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.Appconf.WATCH_FILE));
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.Appconf.WATCH_FILE, "/tmp/" + MAGIC+MAGIC + ".log");
        assertFalse(appnode.checkAllFilesExist());
        expectedFile.delete();
    }

    @Test
    public void testAssignCollectorProcess() throws InterruptedException {
        Message bad = getMessageOfAssignCollector(MAGIC + MAGIC);
        assertFalse(appnode.process(bad));
        Message good = getMessageOfAssignCollector(MAGIC);
        assertTrue(appnode.process(good));
    }
    
    @Test
    public void testRecoveryRollProcess() throws InterruptedException {
        Message bad = getMessageOfRecoveryRoll(MAGIC + MAGIC);
        assertFalse(appnode.process(bad));
        Message good = getMessageOfRecoveryRoll(MAGIC);
        assertTrue(appnode.process(good));
    }
    
    @Test
    public void testUnknowMessageProcess() throws InterruptedException {
        Message unknow = getUnknowMessage();
        assertFalse(appnode.process(unknow));
    }

    private Message getMessageOfAssignCollector(String appName) {
        return PBwrap.wrapAssignCollector(appName, SimAppnode.HOSTNAME, SimAppnode.COLPORT);
    }
    
    private Message getMessageOfRecoveryRoll(String appName) {
        return PBwrap.wrapRecoveryRoll(appName, SimAppnode.HOSTNAME, SimAppnode.COLPORT, SimAppnode.rollTS);
    }

    private Message getUnknowMessage() {
        return PBwrap.wrapReadyCollector(MAGIC, SimAppnode.HOSTNAME, 3600l, SimAppnode.HOSTNAME, 1l);
    }
}

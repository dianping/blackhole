package com.dp.blackhole.appnode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.protocol.control.MessagePB.Message;

public class TestAppnode {
    private static String MAGIC;
    static {
        try {
            MAGIC = Util.getLocalHost().substring(0, 2);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
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
     * WATCH_FILE: "/tmp/check1/name.access.log /tmp/check2/name.access.log"
     * expectedFileName1: "/tmp/check1/name.access.log"
     * expectedFileName2: "/tmp/check2/name.access.log"
     * expectedFileName3: "/tmp/check1/hostname.nh.access.log"
     * expectedFileName4: "/tmp/check2/hostname.nh.access.log"
     * expectedFileName5: "/tmp/check2/xxxx.access.log"
     */
    @Test
    public void testCheckAllFilesExist() throws IOException {
        new File("/tmp/check1").mkdir();
        new File("/tmp/check2").mkdir();
        String hostname = Util.getLocalHost();
        String name = hostname.substring(0, 2);
        String WATCH_FILE = "/tmp/check1/" + name + ".access.log /tmp/check2/" + name + ".access.log";
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.Appconf.WATCH_FILE, WATCH_FILE);
        String expectedFileName1 = "/tmp/check1/" + name + ".access.log";
        String expectedFileName2 = "/tmp/check2/" + name + ".access.log";
        String expectedFileName3 = "/tmp/check1/" + hostname + ".access.log";
        String expectedFileName4 = "/tmp/check2/" + hostname + ".access.log";
        String expectedFileName5 = "/tmp/check2/xxxxxxx.access.log";
        File expectedFile;
        expectedFile = new File(expectedFileName1);
        expectedFile.createNewFile();
        assertTrue(appnode.checkAllFilesExist());
        assertEquals(expectedFileName1, ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.Appconf.WATCH_FILE));
        expectedFile.delete();
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.Appconf.WATCH_FILE, WATCH_FILE);
        expectedFile = new File(expectedFileName2);
        expectedFile.createNewFile();
        assertTrue(appnode.checkAllFilesExist());
        assertEquals(expectedFileName2, ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.Appconf.WATCH_FILE));
        expectedFile.delete();
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.Appconf.WATCH_FILE, WATCH_FILE);
        expectedFile = new File(expectedFileName3);
        expectedFile.createNewFile();
        assertTrue(appnode.checkAllFilesExist());
        assertEquals(expectedFileName3, ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.Appconf.WATCH_FILE));
        expectedFile.delete();
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.Appconf.WATCH_FILE, WATCH_FILE);
        expectedFile = new File(expectedFileName4);
        expectedFile.createNewFile();
        assertTrue(appnode.checkAllFilesExist());
        assertEquals(expectedFileName4, ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.Appconf.WATCH_FILE));
        expectedFile.delete();
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.Appconf.WATCH_FILE, WATCH_FILE);
        expectedFile = new File(expectedFileName5);
        expectedFile.createNewFile();
        assertFalse(appnode.checkAllFilesExist());
        expectedFile.delete();
        new File("/tmp/check1").delete();
        new File("/tmp/check2").delete();
    }

    @Test
    public void testAssignCollectorProcess() throws InterruptedException {
        Message bad = getMessageOfAssignCollector(MAGIC + MAGIC);
        assertFalse(appnode.processor.processInternal(bad));
        Message good = getMessageOfAssignCollector(MAGIC);
        assertTrue(appnode.processor.processInternal(good));
    }
    
    @Test
    public void testRecoveryRollProcess() throws InterruptedException {
        Message bad = getMessageOfRecoveryRoll(MAGIC + MAGIC);
        assertFalse(appnode.processor.processInternal(bad));
        Message good = getMessageOfRecoveryRoll(MAGIC);
        assertTrue(appnode.processor.processInternal(good));
    }
    
    @Test
    public void testUnknowMessageProcess() throws InterruptedException {
        Message unknow = getUnknowMessage();
        assertFalse(appnode.processor.processInternal(unknow));
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

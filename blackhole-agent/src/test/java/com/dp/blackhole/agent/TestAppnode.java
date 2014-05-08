package com.dp.blackhole.agent;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.conf.Context;
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

    @Before
    public void setUp() throws Exception {
        ConfigKeeper.configMap.clear();
        File tailFile = new File("/tmp/" + MAGIC + ".log");
        tailFile.createNewFile();
        tailFile.deleteOnExit();
        appnode = new SimAppnode();
        ConfigKeeper.configMap.put(MAGIC, new Context(ParamsKey.Appconf.ROLL_PERIOD, "3600"));
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.Appconf.MAX_LINE_SIZE, "1024");
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.Appconf.WATCH_FILE, tailFile.getAbsolutePath());
    }

    @After
    public void tearDown() throws Exception {
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
        String expectedFileName1 = "/tmp/check1/" + name + ".access.log";
        String expectedFileName2 = "/tmp/check2/" + name + ".access.log";
        String expectedFileName3 = "/tmp/check1/" + hostname + ".access.log";
        String expectedFileName4 = "/tmp/check2/" + hostname + ".access.log";
        String expectedFileName5 = "/tmp/check2/xxxxxxx.access.log";
        File expectedFile;
        expectedFile = new File(expectedFileName1);
        expectedFile.createNewFile();
        assertTrue(appnode.checkFilesExist(MAGIC, WATCH_FILE));
        assertEquals(expectedFileName1, ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.Appconf.WATCH_FILE));
        expectedFile.delete();
        expectedFile = new File(expectedFileName2);
        expectedFile.createNewFile();
        assertTrue(appnode.checkFilesExist(MAGIC, WATCH_FILE));
        assertEquals(expectedFileName2, ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.Appconf.WATCH_FILE));
        expectedFile.delete();
        expectedFile = new File(expectedFileName3);
        expectedFile.createNewFile();
        assertTrue(appnode.checkFilesExist(MAGIC, WATCH_FILE));
        assertEquals(expectedFileName3, ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.Appconf.WATCH_FILE));
        expectedFile.delete();
        expectedFile = new File(expectedFileName4);
        expectedFile.createNewFile();
        assertTrue(appnode.checkFilesExist(MAGIC, WATCH_FILE));
        assertEquals(expectedFileName4, ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.Appconf.WATCH_FILE));
        expectedFile.delete();
        expectedFile = new File(expectedFileName5);
        expectedFile.createNewFile();
        assertFalse(appnode.checkFilesExist(MAGIC, WATCH_FILE));
        expectedFile.delete();
        new File("/tmp/check1").delete();
        new File("/tmp/check2").delete();
    }

    @Test
    public void testAssignCollectorProcess() throws InterruptedException {
        appnode.fillUpAppLogsFromConfig(MAGIC);
        Message bad = getMessageOfAssignCollector(MAGIC + MAGIC);
        assertFalse(appnode.processor.processInternal(bad));
        Message good = getMessageOfAssignCollector(MAGIC);
        assertTrue(appnode.processor.processInternal(good));
    }
    
    @Test
    public void testRecoveryRollProcess() throws InterruptedException {
        appnode.fillUpAppLogsFromConfig(MAGIC);
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

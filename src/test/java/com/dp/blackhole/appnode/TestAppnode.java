package com.dp.blackhole.appnode;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.simutil.SimAppnode;
import com.dp.blackhole.simutil.Util;
public class TestAppnode {
    private static final String MAGIC = "9vjrder3";
    private SimAppnode appnode;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        ConfigKeeper conf = new ConfigKeeper();
        conf.addRawProperty(MAGIC+".WATCH_FILE", "/tmp/" + MAGIC + ".log");
        conf.addRawProperty(MAGIC+".ROLL_PERIOD", "3600");
        conf.addRawProperty(MAGIC+".BUFFER_SIZE", "4096");
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
        return PBwrap.wrapAssignCollector(appName, Util.HOSTNAME, Util.COLPORT);
    }
    
    private Message getMessageOfRecoveryRoll(String appName) {
        return PBwrap.wrapRecoveryRoll(appName, Util.HOSTNAME, Util.COLPORT, Util.rollTS);
    }

    private Message getUnknowMessage() {
        return PBwrap.wrapReadyCollector(MAGIC, Util.HOSTNAME, 3600l, Util.HOSTNAME, 1l);
    }
}

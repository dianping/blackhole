package com.dp.blackhole.appnode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.appnode.Appnode;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.simutil.Util;
public class TestAppnode {
    private static final Log LOG = LogFactory.getLog(TestAppnode.class);
    private static final String MAGIC = "9vjrder3";
    private static Appnode appnode;
    private static String client;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        try {
            client = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            LOG.error("Oops, got an exception:", e1);
            return;
        }
        appnode = new Appnode(client);
        ConfigKeeper conf = new ConfigKeeper();
        conf.addRawProperty(MAGIC+".WATCH_FILE", "/tmp/" + MAGIC + ".log");
        conf.addRawProperty(MAGIC+".port", "40000");//TODO
        conf.addRawProperty(MAGIC+".TRANSFER_PERIOD_VALUE", "1");
        conf.addRawProperty(MAGIC+".TRANSFER_PERIOD_UNIT", "hour");
        appnode.fillUpAppLogsFromConfig();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testAssignCollectorProcess() {
        Message bad = getMessageOfAssignCollector(MAGIC + MAGIC);
        assertFalse(appnode.process(bad));
        Message good = getMessageOfAssignCollector(MAGIC);
        assertTrue(appnode.process(good));
    }
    
    @Test
    public void testRecoveryRollProcess() {
        Message bad = getMessageOfRecoveryRoll(MAGIC + MAGIC);
        assertFalse(appnode.process(bad));
        Message good = getMessageOfRecoveryRoll(MAGIC);
        assertTrue(appnode.process(good));
    }
    
    @Test
    public void testUnknowMessageProcess() {
        Message unknow = getUnknowMessage();
        assertFalse(appnode.process(unknow));
    }

    private Message getMessageOfAssignCollector(String appName) {
        return PBwrap.wrapAssignCollector(appName, Util.HOSTNAME);
    }
    
    private Message getMessageOfRecoveryRoll(String appName) {
        return PBwrap.wrapRecoveryRoll(appName, Util.HOSTNAME, Util.rollTS);
    }

    private Message getUnknowMessage() {
        return PBwrap.wrapReadyCollector(MAGIC, Util.HOSTNAME, Util.HOSTNAME, 1l);
    }
    
    @Test
    public void testLoadLocalConfig() throws ParseException, IOException {
        File confFile = File.createTempFile("app.conf", null);
        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(confFile, true));
        writer.write("testApp.WATCH_FILE = /tmp/testApp.log\n");
        writer.write("testApp.second = sencond preperty\n");
        writer.flush();
        writer.close();
        
        String[] args = new String[2];
        args[0] = "-f";
        args[1] = confFile.getAbsolutePath();
        Appnode appnode = new Appnode(client);
        appnode.setArgs(args);
        assertTrue(appnode.parseOptions());
        appnode.loadLocalConfig();
        assertTrue(ConfigKeeper.configMap.containsKey("testApp"));
        String path = ConfigKeeper.configMap.get("testApp")
                .getString(ParamsKey.Appconf.WATCH_FILE);
        assertEquals(path, "/tmp/testApp.log");
        assertTrue(ConfigKeeper.configMap.containsKey("testApp"));
        String second = ConfigKeeper.configMap.get("testApp")
                .getString("second");
        assertEquals(second, "sencond preperty");
        confFile.delete();
    }
}

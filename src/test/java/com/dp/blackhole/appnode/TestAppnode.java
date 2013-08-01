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

import com.dp.blackhole.common.AssignCollectorPB.AssignCollector;
import com.dp.blackhole.common.MessagePB.Message;
import com.dp.blackhole.common.MessagePB.Message.MessageType;
import com.dp.blackhole.common.ReadyCollectorPB.ReadyCollector;
import com.dp.blackhole.common.RecoveryRollPB.RecoveryRoll;
import com.dp.blackhole.conf.AppConfigurationConstants;
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
        conf.addRawProperty(MAGIC+".watchFile", "/tmp/" + MAGIC + ".log");
        conf.addRawProperty(MAGIC+".port", "40000");
        conf.addRawProperty(MAGIC+".transferPeriodValue", "1");
        conf.addRawProperty(MAGIC+".transferPeriodUnit", "hour");
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
        AssignCollector.Builder assignCollectorBuilder = AssignCollector.newBuilder();
        assignCollectorBuilder.setAppName(appName);
        assignCollectorBuilder.setCollectorServer(Util.HOSTNAME);
        AssignCollector assignCollector = assignCollectorBuilder.build();
        Message.Builder messageBuilder = Message.newBuilder();
        messageBuilder.setType(MessageType.ASSIGN_COLLECTOR);
        messageBuilder.setAssignCollector(assignCollector);
        Message message = messageBuilder.build();
        return message;
    }
    
    private Message getMessageOfRecoveryRoll(String appName) {
        RecoveryRoll.Builder recoveryRollBuilder = RecoveryRoll.newBuilder();
        recoveryRollBuilder.setAppName(appName);
        recoveryRollBuilder.setCollectorServer(Util.HOSTNAME);
        recoveryRollBuilder.setRollTs(Util.rollTS);
        recoveryRollBuilder.setOffset(0l);
        RecoveryRoll recoveryRoll = recoveryRollBuilder.build();
        Message.Builder messageBuilder = Message.newBuilder();
        messageBuilder.setType(MessageType.RECOVERY_ROLL);
        messageBuilder.setRecoveryRoll(recoveryRoll);
        Message message = messageBuilder.build();
        return message;
    }

    private Message getUnknowMessage() {
        ReadyCollector.Builder readyCollectorBuilder = ReadyCollector.newBuilder();
        readyCollectorBuilder.setAppName(MAGIC);
        readyCollectorBuilder.setAppServer(Util.HOSTNAME);
        readyCollectorBuilder.setCollectorServer(Util.HOSTNAME);
        readyCollectorBuilder.setConnectedTs(1l);
        ReadyCollector readyCollector = readyCollectorBuilder.build();
        Message.Builder messageBuilder = Message.newBuilder();
        messageBuilder.setType(MessageType.READY_COLLECTOR);
        messageBuilder.setReadyCollector(readyCollector);
        Message message = messageBuilder.build();
        return message;
    }
    
    @Test
    public void testLoadLocalConfig() throws ParseException, IOException {
        File confFile = File.createTempFile("app.conf", null);
        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(confFile, true));
        writer.write("testApp.watchFile = /tmp/testApp.log\n");
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
                .getString(AppConfigurationConstants.WATCH_FILE);
        assertEquals(path, "/tmp/testApp.log");
        assertTrue(ConfigKeeper.configMap.containsKey("testApp"));
        String second = ConfigKeeper.configMap.get("testApp")
                .getString("second");
        assertEquals(second, "sencond preperty");
        confFile.delete();
    }
}

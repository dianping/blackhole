package com.dp.blackhole.appnode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
import com.dp.blackhole.common.RecoveryCollectorPB.RecoveryCollector;
import com.dp.blackhole.common.RecoveryRollPB.RecoveryRoll;
import com.dp.blackhole.conf.AppConfigurationConstants;
import com.dp.blackhole.conf.Configuration;

public class TestAppnode {
    private static final Log LOG = LogFactory.getLog(TestAppnode.class);
    private static final String rollIdent = "2013-01-01.15:00:15";
    private static final int offset = 100;
    private String client;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        try {
            client = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            LOG.error("Oops, got an exception:", e1);
            return;
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testProcess() {
        //she zhe yige applog fang dao applogs zhong
        Appnode appnode = new Appnode(client);
        Message message = getMessageOfAssignCollector();
        Message message2 = getMessageOfRecoveryCollector();
        Message message3 = getMessageOfRecoveryRoll();
        appnode.process(message);
//        appnode.process(message2);
//        appnode.process(message3);
    }

    private Message getMessageOfAssignCollector() {
        AssignCollector.Builder assignCollectorBuilder = AssignCollector.newBuilder();
        assignCollectorBuilder.setAppName("testApp");
        assignCollectorBuilder.setCollectorServer("localhost");
        assignCollectorBuilder.setCollectorPort(40000);
        AssignCollector assignCollector = assignCollectorBuilder.build();
        Message.Builder messageBuilder = Message.newBuilder();
        messageBuilder.setType(MessageType.ASSIGN_COLLECTOR);
        messageBuilder.setAssignCollector(assignCollector);
        Message message = messageBuilder.build();
        return message;
    }
    
    private Message getMessageOfRecoveryCollector() {
        RecoveryCollector.Builder recoveryCollectorBuilder = RecoveryCollector.newBuilder();
        recoveryCollectorBuilder.setAppName("testApp");
        recoveryCollectorBuilder.setCollectorServer("localhost");
        recoveryCollectorBuilder.setCollectorPort(40001);
        RecoveryCollector recoveryCollector = recoveryCollectorBuilder.build();
        Message.Builder messageBuilder = Message.newBuilder();
        messageBuilder.setType(MessageType.RECOVERY_COLLECTOR);
        messageBuilder.setRecoveryCollector(recoveryCollector);
        Message message = messageBuilder.build();
        return message;
    }
    
    private Message getMessageOfRecoveryRoll() {
        RecoveryRoll.Builder recoveryRollBuilder = RecoveryRoll.newBuilder();
        recoveryRollBuilder.setAppName("testApp");
        recoveryRollBuilder.setCollectorServer("localhost");
        recoveryRollBuilder.setCollectorPort(40000);
        recoveryRollBuilder.setRollIdent(rollIdent);
        recoveryRollBuilder.setOffset(offset);
        RecoveryRoll recoveryRoll = recoveryRollBuilder.build();
        Message.Builder messageBuilder = Message.newBuilder();
        messageBuilder.setType(MessageType.RECOVERY_ROLL);
        messageBuilder.setRecoveryRoll(recoveryRoll);
        Message message = messageBuilder.build();
        return message;
    }

    private Message getMessageOfUnknow() {
        RecoveryCollector.Builder recoveryCollectorBuilder = RecoveryCollector.newBuilder();
        recoveryCollectorBuilder.setAppName("testApp");
        recoveryCollectorBuilder.setCollectorServer("localhost");
        recoveryCollectorBuilder.setCollectorPort(40001);
        RecoveryCollector recoveryCollector = recoveryCollectorBuilder.build();
        Message.Builder messageBuilder = Message.newBuilder();
        messageBuilder.setType(MessageType.REPLY_COLLECTOR);
        messageBuilder.setRecoveryCollector(recoveryCollector);
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
        assertTrue(Configuration.configMap.containsKey("testApp"));
        String path = Configuration.configMap.get("testApp")
                .getString(AppConfigurationConstants.WATCH_FILE);
        assertEquals(path, "/tmp/testApp.log");
        assertTrue(Configuration.configMap.containsKey("testApp"));
        String second = Configuration.configMap.get("testApp")
                .getString("second");
        assertEquals(second, "sencond preperty");
        confFile.deleteOnExit();
    }
}

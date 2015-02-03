package com.dp.blackhole.agent;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.agent.TopicMeta.TopicId;
import com.dp.blackhole.broker.BrokerService;
import com.dp.blackhole.broker.SimBroker;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.conf.Context;
import com.dp.blackhole.protocol.control.MessagePB.Message;

public class TestAgent {
    private TopicId topicId;
    private TopicMeta topicMeta;
    private static String MAGIC;
    static {
        try {
            MAGIC = Util.getLocalHost().substring(0, 2);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    private static final String tmpDir = "/tmp/" + MAGIC + "/base";
    
    private SimAgent agent;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        //build a server
        Properties properties = new Properties();
        properties.setProperty("GenServer.handlercount", "1");
        properties.setProperty("broker.service.port", "" + "40008");
        properties.setProperty("broker.storage.dir", tmpDir);
        BrokerService pubservice = new BrokerService(properties);
        new SimBroker(40008);
        SimBroker.getRollMgr().init("/tmp/hdfs", "gz", 40008, 5000, 1, 1, 60000);
        pubservice.start();
    }
    
    @Before
    public void setUp() throws Exception {
        ConfigKeeper.configMap.clear();
        File tailFile = new File("/tmp/" + MAGIC + ".log");
        tailFile.createNewFile();
        tailFile.deleteOnExit();
        agent = new SimAgent();
        ConfigKeeper.configMap.put(MAGIC, new Context(ParamsKey.TopicConf.ROTATE_PERIOD, "3600"));
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.TopicConf.ROLL_PERIOD, "3600");
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.TopicConf.MAX_LINE_SIZE, "1024");
        ConfigKeeper.configMap.get(MAGIC).put(ParamsKey.TopicConf.WATCH_FILE, tailFile.getAbsolutePath());
        topicId = new TopicId(MAGIC, null);
        topicMeta = agent.fillUpAppLogsFromConfig(topicId);
        Map<TopicMeta, LogReader> map = Agent.getTopicReaders();
        map.put(topicMeta, new LogReader(agent, topicMeta, "/tmp/" + MAGIC));
    }

    @After
    public void tearDown() throws Exception {
        Map<TopicMeta, LogReader> map = Agent.getTopicReaders();
        map.clear();
        SimAgent.deleteTmpFile(MAGIC);
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
        String expectedFileName5 = "/tmp/check2/xxxxxxx.access.log";
        File expectedFile;
        expectedFile = new File(expectedFileName1);
        expectedFile.createNewFile();
        assertTrue(agent.checkFilesExist(MAGIC, WATCH_FILE));
        assertEquals(expectedFileName1, ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.TopicConf.WATCH_FILE));
        expectedFile.delete();
        expectedFile = new File(expectedFileName2);
        expectedFile.createNewFile();
        assertTrue(agent.checkFilesExist(MAGIC, WATCH_FILE));
        assertEquals(expectedFileName2, ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.TopicConf.WATCH_FILE));
        expectedFile.delete();
        expectedFile = new File(expectedFileName5);
        expectedFile.createNewFile();
        assertFalse(agent.checkFilesExist(MAGIC, WATCH_FILE));
        expectedFile.delete();
        new File("/tmp/check1").delete();
        new File("/tmp/check2").delete();
    }

    @Test
    public void testAssignBrokerProcess() throws InterruptedException {
        topicMeta = agent.fillUpAppLogsFromConfig(topicId);
        Message bad = getMessageOfAssignBroker(MAGIC + MAGIC);
        assertFalse(agent.processor.processInternal(bad));
        Message good = getMessageOfAssignBroker(MAGIC);
        assertTrue(agent.processor.processInternal(good));
    }
    
    @Test
    public void testRecoveryRollProcess() throws InterruptedException {
        topicId = new TopicId(MAGIC, null);
        Message bad = getMessageOfRecoveryRoll(MAGIC + MAGIC);
        assertFalse(agent.processor.processInternal(bad));
        Message good = getMessageOfRecoveryRoll(MAGIC);
        assertTrue(agent.processor.processInternal(good));
    }
    
    @Test
    public void testUnknowMessageProcess() throws InterruptedException {
        Message unknow = getUnknowMessage();
        assertFalse(agent.processor.processInternal(unknow));
    }

    private Message getMessageOfAssignBroker(String appName) {
        return PBwrap.wrapAssignBroker(appName, SimAgent.HOSTNAME, 40008, null);
    }
    
    private Message getMessageOfRecoveryRoll(String appName) {
        return PBwrap.wrapRecoveryRoll(appName, SimAgent.HOSTNAME, SimAgent.COLPORT, SimAgent.rollTS, null, false);
    }

    private Message getUnknowMessage() {
        return PBwrap.wrapReadyStream(MAGIC, SimAgent.HOSTNAME, 3600l, SimAgent.HOSTNAME, 1l);
    }
}

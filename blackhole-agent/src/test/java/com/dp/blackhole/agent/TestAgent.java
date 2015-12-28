package com.dp.blackhole.agent;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import com.dp.blackhole.agent.AgentMeta.TopicId;
import com.dp.blackhole.broker.BrokerService;
import com.dp.blackhole.broker.SimBroker;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.conf.Context;
import com.dp.blackhole.protocol.control.MessagePB.Message;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"org.apache.hadoop.*", "com.sun.*", "net.contentobjects.*"})
public class TestAgent {
    private TopicId topicId;
    private AgentMeta topicMeta;
    private static String MAGIC;
    static {
        MAGIC = Util.getLocalHost().substring(0, 2);
    }
    private static final String tmpDir = "/tmp/" + MAGIC + "/base";
    
    private SimAgent agent;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        //build a server
        Properties properties = new Properties();
        properties.setProperty("GenServer.handlercount", "1");
        properties.setProperty("broker.service.port", "" + "40010");
        properties.setProperty("broker.storage.dir", tmpDir);
        BrokerService pubservice = new BrokerService(properties);
        properties.setProperty("broker.storage.splitThreshold", "536870912");
        properties.setProperty("broker.storage.flushThreshold", "4096");
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
        Map<AgentMeta, LogReader> map = Agent.getTopicReaders();
        map.put(topicMeta, new LogReader(agent, topicMeta, "/tmp/" + MAGIC));
    }

    @After
    public void tearDown() throws Exception {
        Map<AgentMeta, LogReader> map = Agent.getTopicReaders();
        map.clear();
        SimAgent.deleteTmpFile(MAGIC);
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

    private Message getMessageOfAssignBroker(String topic) {
        return PBwrap.wrapAssignBroker(PBwrap.assignBroker(topic, SimAgent.HOSTNAME, 40008, null, null));
    }
    
    private Message getMessageOfRecoveryRoll(String appName) {
        return PBwrap.wrapRecoveryRoll(appName, SimAgent.HOSTNAME, SimAgent.COLPORT, SimAgent.rollTS, SimAgent.HOSTNAME, false, true);
    }

    private Message getUnknowMessage() {
        return PBwrap.wrapReadyStream(MAGIC, SimAgent.HOSTNAME, 3600l, SimAgent.HOSTNAME, 1l);
    }
}

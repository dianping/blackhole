package com.dp.blackhole.agent;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.agent.TopicMeta;
import com.dp.blackhole.agent.TopicMeta.TopicId;
import com.dp.blackhole.agent.persist.LocalRecorder;
import com.dp.blackhole.agent.persist.Record;
import com.dp.blackhole.conf.ConfigKeeper;

public class TestRollRecovery {
    private static final String MAGIC = "ctg4ewd";
    private static final int port = 40002;
    private static File file;
    private static TopicMeta appLog;
    private static List<String> header = new ArrayList<String>();
    private static List<String> receives = new ArrayList<String>();
    private Thread serverThread;
    private static SimAgent agent;
    private LocalRecorder state;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        agent = new SimAgent();
        ConfigKeeper confKeeper = new ConfigKeeper();
        confKeeper.addRawProperty(MAGIC + ".rollPeriod", "3600");
        confKeeper.addRawProperty(MAGIC + ".maxLineSize", "1024");
        //build a tmp file
        file = SimAgent.createTmpFile(MAGIC + "." + SimAgent.FILE_SUFFIX, SimAgent.expected);
        file = SimAgent.createTmpFile(MAGIC, SimAgent.expected);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        SimAgent.deleteTmpFile(MAGIC);
    }

    @Before
    public void setUp() throws Exception {
        state = mock(LocalRecorder.class);
        when(state.retrive(SimAgent.rollTS)).thenReturn(new Record(Record.ROTATE, SimAgent.rollTS, LogReader.BEGIN_OFFSET_OF_FILE, LogReader.END_OFFSET_OF_FILE));//or 598
        
        TopicId topicId = new TopicId(MAGIC, null);
        appLog = new TopicMeta(topicId, file.getAbsolutePath(), 3600, 3600, 1024, 1L, 5, 4096, 1024*1024);
        SimRecoveryServer server = new SimRecoveryServer(port, header, receives);
        serverThread = new Thread(server);
        serverThread.start();
    }

    @After
    public void tearDown() throws Exception {
//        serverThread.interrupt();
    }

    @Test
    public void test() {
        RollRecovery recovery = new RollRecovery(agent, SimAgent.HOSTNAME, port, appLog, SimAgent.rollTS, false, true, state);
        Thread thread = new Thread(recovery);
        thread.start();
        try {
            serverThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String[] expectedHeader = new String[4];
        expectedHeader[0] = "false";
        expectedHeader[1] = MAGIC;
        expectedHeader[2] = "3600";
        expectedHeader[3] = String.valueOf(SimAgent.rollTS);
        assertArrayEquals("head not match", expectedHeader, header.toArray());
        assertEquals("loader function fail.", SimAgent.expected, receives.get(receives.size()-1));
    }
}

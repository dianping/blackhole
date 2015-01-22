package com.dp.blackhole.agent;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.dp.blackhole.agent.LogReader.EventWriter;
import com.dp.blackhole.agent.TopicMeta;
import com.dp.blackhole.agent.FileListener;
import com.dp.blackhole.agent.LogReader;
import com.dp.blackhole.agent.TopicMeta.TopicId;
import com.dp.blackhole.agent.persist.LocalState;
import com.dp.blackhole.agent.persist.Record;
import com.dp.blackhole.broker.BrokerService;
import com.dp.blackhole.broker.ByteBufferChannel;
import com.dp.blackhole.broker.SimBroker;
import com.dp.blackhole.broker.storage.Segment;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.FileMessageSet;
import com.dp.blackhole.storage.MessageAndOffset;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"org.apache.hadoop.*", "com.sun.*", "net.contentobjects.*"})
@PrepareForTest(LocalState.class)
public class TestLogReader {
    private static final String MAGIC = "sdfjiojwe";
    private static final int port = 40001;
    private Thread loggerThread;
    private ArrayList<String> expectedLines;
    private static final String tmpDir = "/tmp/" + MAGIC + "/base";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        ConfigKeeper confKeeper = new ConfigKeeper();
        confKeeper.addRawProperty(MAGIC + ".rollPeriod", "3600");
        //build a server
        Properties properties = new Properties();
        properties.setProperty("GenServer.handlercount", "1");
        properties.setProperty("broker.service.port", "40001");
        properties.setProperty("broker.storage.dir", tmpDir);
        BrokerService pubservice = new BrokerService(properties);
        new SimBroker(port);
        SimBroker.getRollMgr().init("/tmp/hdfs", "gz", 40020, 5000, 1, 1, 60000);
        pubservice.start();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        //build a app log
        SimLogger logger = new SimLogger(100);
        loggerThread = new Thread(logger);
        loggerThread.start();
        expectedLines = logger.getVerifyLines();
    }

    @After
    public void tearDown() throws Exception {
        loggerThread.interrupt();
        SimAgent.deleteTmpFile(SimLogger.TEST_ROLL_FILE_NAME);
        SimAgent.deleteTmpFile(MAGIC);
    }

    @Test
    public void testFileRotated() throws IOException {
        String localhost = Util.getLocalHost();
        TopicId topicId = new TopicId(MAGIC, null);
        TopicMeta appLog = new TopicMeta(topicId, SimAgent.TEST_ROLL_FILE, 3600, 3600, 1024, 1L, 5, 4096);
        SimAgent agent = new SimAgent();
        FileListener listener;
        try {
            listener = new FileListener();
        } catch (Exception e) {
            System.err.println(e);
            return;
        }
        agent.setListener(listener);
        Thread readerThread = null;
        try {
            LogReader reader = new LogReader(agent, SimAgent.HOSTNAME, localhost, port, appLog, "/tmp");
            readerThread = new Thread(reader);
            readerThread.start();
            Thread.sleep(1000);
            reader.doFileAppendForce();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        readerThread.interrupt();
        ByteBuffer buffer = ByteBuffer.allocate(1024*1024);
        ByteBufferChannel channel = new ByteBufferChannel(buffer);
        Segment segment = new Segment(tmpDir + "/" + MAGIC + "/"+ localhost, 0, false, false, 1024, 108);
        FileMessageSet fms = segment.read(0, 1024*1024);
        fetchFileMessageSet(channel, fms);
        buffer.flip();
        ByteBufferMessageSet bms = new ByteBufferMessageSet(buffer, 0);
        Iterator<MessageAndOffset> iter = bms.getItertor();
        Charset charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = charset.newDecoder();
        MessageAndOffset mo = null;
        while (iter.hasNext()) {
            mo = iter.next();
        }
        assertEquals(expectedLines.get(expectedLines.size() - 2), decoder.decode(mo.message.payload()).toString());
    }
    private void fetchFileMessageSet(GatheringByteChannel channel, FileMessageSet messages) throws IOException {
        int read = 0;
        int limit = messages.getSize();
        while (read < limit) {
            read += fetchChunk(channel, messages, read, limit - read);
        }
    }

    private int fetchChunk(GatheringByteChannel channel, FileMessageSet messages, int start, int limit) throws IOException {
        int read = 0;
        while (read < limit) {
            read += messages.write(channel, start + read, limit - read);
        }
        return read;
    }
    
    @Test
    public void testRestoreMissingRotationRecords() {
        LogReader reader = mock(LogReader.class);
        TopicId topicId = new TopicId(MAGIC, null);
        TopicMeta meta = new TopicMeta(topicId, SimAgent.TEST_ROLL_FILE, 3600, 3600, 1024, 1L, 5, 4096);
        LocalState spyState = spy(new LocalState("/tmp/test1", meta));
        try {
            doNothing().when(spyState, "persist");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
//            loggerThread.start();
            EventWriter eventWriter = reader.new EventWriter(spyState, new File(SimLogger.TEST_ROLL_FILE), 1024, 1);
            long rollTs1 = 1421036400000L; //2015-01-12 12:20:00
            spyState.record(Record.ROLL, rollTs1, 100);
            long rollTs2 = 1421036700000L; //2015-01-12 12:25:00
            spyState.record(Record.ROLL, rollTs2, 200);
            long resumeRollTs = 1421065320000L; //2015-01-12 20:22:00
            eventWriter.restoreMissingRotationRecords(300, 3600, resumeRollTs);
            assertEquals(10, spyState.getSnapshot().getRecords().size());
            assertEquals(1421036400000L, spyState.getSnapshot().getRecords().get(0).getRollTs());
            assertEquals(1421036700000L, spyState.getSnapshot().getRecords().get(1).getRollTs());
            assertEquals(1421038500000L, spyState.getSnapshot().getRecords().get(2).getRollTs());
            assertEquals(1421042100000L, spyState.getSnapshot().getRecords().get(3).getRollTs());
            assertEquals(1421045700000L, spyState.getSnapshot().getRecords().get(4).getRollTs());
            assertEquals(1421049300000L, spyState.getSnapshot().getRecords().get(5).getRollTs());
            assertEquals(1421052900000L, spyState.getSnapshot().getRecords().get(6).getRollTs());
            assertEquals(1421056500000L, spyState.getSnapshot().getRecords().get(7).getRollTs());
            assertEquals(1421060100000L, spyState.getSnapshot().getRecords().get(8).getRollTs());
            assertEquals(1421063700000L, spyState.getSnapshot().getRecords().get(9).getRollTs());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}

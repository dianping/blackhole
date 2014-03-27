package com.dp.blackhole.appnode;

import static org.junit.Assert.*;

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

import com.dp.blackhole.appnode.AppLog;
import com.dp.blackhole.appnode.FileListener;
import com.dp.blackhole.appnode.LogReader;
import com.dp.blackhole.collectornode.BrokerService;
import com.dp.blackhole.collectornode.ByteBufferChannel;
import com.dp.blackhole.collectornode.SimCollectornode;
import com.dp.blackhole.collectornode.persistent.Segment;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.FileMessageSet;
import com.dp.blackhole.storage.MessageAndOffset;

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
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        //build a server
        Properties properties = new Properties();
        properties.setProperty("GenServer.handlercount", "1");
        properties.setProperty("broker.service.port", "40001");
        properties.setProperty("broker.storage.dir", tmpDir);
        BrokerService pubservice = new BrokerService(properties);
        new SimCollectornode(port);
        SimCollectornode.getRollMgr().init("/tmp/hdfs", ".gz", 40020, 5000);
        pubservice.start();

        //build a app log
        SimLogger logger = new SimLogger(100);
        loggerThread = new Thread(logger);
        expectedLines = logger.getVerifyLines();
    }

    @After
    public void tearDown() throws Exception {
        loggerThread.interrupt();
        SimAppnode.deleteTmpFile(MAGIC);
    }

    @Test
    public void testFileRotated() throws IOException {
        AppLog appLog = new AppLog(MAGIC, SimAppnode.TEST_ROLL_FILE, 3600, 1024);
        SimAppnode appnode = new SimAppnode();
        FileListener listener;
        try {
            listener = new FileListener();
        } catch (Exception e) {
            System.err.println(e);
            return;
        }
        appnode.setListener(listener);
        loggerThread.start();
        Thread readerThread = null;
        try {
            Thread.sleep(500);
            LogReader reader = new LogReader(appnode, SimAppnode.HOSTNAME, "localhost",
                    port, appLog);
            readerThread = new Thread(reader);
            Thread.sleep(1000);//ignore file first create
            readerThread.start();
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        readerThread.interrupt();
        ByteBuffer buffer = ByteBuffer.allocate(1024*1024);
        ByteBufferChannel channel = new ByteBufferChannel(buffer);
        Segment segment = new Segment(tmpDir + "/" + MAGIC + "/localhost", 0, false, false, 1024, 108);
        FileMessageSet fms = segment.read(0, 1024*1024);
        fetchFileMessageSet(channel, fms);
        buffer.flip();
        ByteBufferMessageSet bms = new ByteBufferMessageSet(buffer, 0);
        Iterator<MessageAndOffset> iter = bms.getItertor();
        Charset charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = charset.newDecoder();
        int index = 10;
        while (iter.hasNext()) {
            MessageAndOffset mo = iter.next();
            assertEquals(expectedLines.get(index++), decoder.decode(mo.message.payload()).toString());
        }
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
}

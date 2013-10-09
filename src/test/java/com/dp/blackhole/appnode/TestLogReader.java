package com.dp.blackhole.appnode;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.appnode.AppLog;
import com.dp.blackhole.appnode.LogReader;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.simutil.SimAppnode;
import com.dp.blackhole.simutil.SimCollectornode;
import com.dp.blackhole.simutil.SimLogger;

public class TestLogReader {
    private static final String MAGIC = "sdfjiojwe";
    private static List<String> receives = new ArrayList<String>();
    private static final int port = 40001;
    private Thread serverThread;
    private Thread loggerThread;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        ConfigKeeper confKeeper = new ConfigKeeper();
        confKeeper.addRawProperty(MAGIC + ".ROLL_PERIOD", "3600");
        confKeeper.addRawProperty(MAGIC + ".BUFFER_SIZE", "100");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        //build a server
        SimCollectornode server = new SimCollectornode("stream", MAGIC, port, receives);
        serverThread = new Thread(server);
        serverThread.start();

        //build a app log
        SimLogger logger = new SimLogger(100);
        loggerThread = new Thread(logger);
    }

    @After
    public void tearDown() throws Exception {
    	loggerThread.interrupt();
//    	serverThread.interrupt();
        com.dp.blackhole.simutil.Util.deleteTmpFile(MAGIC);
    }

    @Test
    public void testFileNotFoundAndFileRotated() {
        AppLog appLog = new AppLog(MAGIC, com.dp.blackhole.simutil.Util.TEST_ROLL_FILE,
        		System.currentTimeMillis());
        LogReader reader = new LogReader(new SimAppnode("locahost", port), com.dp.blackhole.simutil.Util.HOSTNAME, 
                port, appLog);
        Thread readerThread = new Thread(reader);
        loggerThread.start();
        try {
        	Thread.sleep(1000);//ignore file first create
        	readerThread.start();
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        readerThread.interrupt();
        assertNotNull("testFileNotFound function fail.", receives.toArray());
        assertEquals(true, receives.size()>10);
    }
}

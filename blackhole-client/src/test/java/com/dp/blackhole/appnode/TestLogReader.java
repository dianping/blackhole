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
import com.dp.blackhole.appnode.FileListener;
import com.dp.blackhole.appnode.LogReader;
import com.dp.blackhole.collectornode.SimCollectornode;
import com.dp.blackhole.conf.ConfigKeeper;
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
        confKeeper.addRawProperty(MAGIC + ".rollPeriod", "3600");
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
    	SimAppnode.deleteTmpFile(MAGIC);
    }

    @Test
    public void testFileRotated() {
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
            LogReader reader = new LogReader(appnode, SimAppnode.HOSTNAME, 
                    port, appLog);
            readerThread = new Thread(reader);
        	Thread.sleep(1000);//ignore file first create
        	readerThread.start();
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        readerThread.interrupt();
        assertNotNull("testFileNotFound function fail.", receives.toArray());
        assertEquals(true, receives.size()>20);
    }
}

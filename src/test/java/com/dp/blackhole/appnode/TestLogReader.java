package com.dp.blackhole.appnode;

import static org.junit.Assert.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.appnode.AppLog;
import com.dp.blackhole.appnode.LogReader;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.simutil.SimAppnode;
import com.dp.blackhole.simutil.SimLogger;
import com.dp.blackhole.simutil.SimTailServer;

public class TestLogReader {
    private static final Log LOG = LogFactory.getLog(TestLogReader.class);
    private static final String MAGIC = "sdfjiojwe";
    private static List<String> receives = new ArrayList<String>();
    private static int before = 0;
    private static int after = 0;
    private SimTailServer server;
    private Thread serverThread;
    private Thread loggerThread;
    
    public static int getBefore() {
        return before;
    }

    public static void setBefore() {
        TestLogReader.before = receives.size();
    }

    public static int getAfter() {
        return after;
    }

    public static void setAfter() {
        TestLogReader.after = receives.size();
    }
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        ConfigKeeper confKeeper = new ConfigKeeper();
        confKeeper.addRawProperty(MAGIC+".port", "40000");
        confKeeper.addRawProperty(MAGIC + ".ROLL_PERIOD", "3600");
        confKeeper.addRawProperty(MAGIC + ".BUFFER_SIZE", "100");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        //build a server
        server = new SimTailServer(com.dp.blackhole.simutil.Util.PORT, receives);
        serverThread = new Thread(server);
        serverThread.start();

        //build a app log
        SimLogger logger = new SimLogger(100);
        loggerThread = new Thread(logger);
    }

    @After
    public void tearDown() throws Exception {
    	serverThread.interrupt();
    	loggerThread.interrupt();
        com.dp.blackhole.simutil.Util.deleteTmpFile(MAGIC);
    }

    @Test
    public void testFileNotFoundAndFileRotated() {
        AppLog appLog = new AppLog(MAGIC, com.dp.blackhole.simutil.Util.TEST_ROLL_FILE,
        		System.currentTimeMillis());
        LogReader reader = new LogReader(new SimAppnode("locahost"), com.dp.blackhole.simutil.Util.HOSTNAME, 
                com.dp.blackhole.simutil.Util.PORT, appLog);
        Thread readerThread = new Thread(reader);
        loggerThread.start();
        try {
        	Thread.sleep(1000);//ignore file first create
        	readerThread.start();
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        readerThread.interrupt();
        assertNotNull("testFileNotFound function fail.", receives.toArray());
        assertNotSame(getBefore(), getAfter());
    }
}

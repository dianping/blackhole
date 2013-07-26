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

import com.dp.blackhole.common.Util;
import com.dp.blackhole.simutil.SimTailServer;

public class TestLogReader {
    private static final Log LOG = LogFactory.getLog(TestLogReader.class);

    private static AppLog appLog;
    private static Appnode appnode;
    private static final String MAGIC = "sdfjiojwe";
    private static List<String> receives = new ArrayList<String>();
    private static int before = 0;
    private static int after = 0;
    private SimTailServer server;
    private Thread serverThread;
    
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
        File file = File.createTempFile(MAGIC, null);
        String fileAbsolutePath = file.getAbsolutePath();
        LOG.info("create tmp file for test LogReader " + file);
        Thread t = new Thread(new Writer(fileAbsolutePath));
        t.setDaemon(false);
        t.start();
        appLog = new AppLog(MAGIC, fileAbsolutePath, System.currentTimeMillis(), 
                "localhost", com.dp.blackhole.simutil.Util.PORT);

        //build a app node
        String appClient = "127.0.0.1";
        appnode = new Appnode(appClient);
    }

    @After
    public void tearDown() throws Exception {
//        List<File> candidateFiles = Arrays.asList(new File("/tmp").listFiles());
//        for (File file : candidateFiles) {
//            if (file.getName().startsWith(MAGIC)) {
//                LOG.info("delete tmp file for test LogReader " + file);
//                file.deleteOnExit();
//            }
//        }
    }

    static class Writer implements Runnable{
        private String path;
        private volatile boolean shouldBreak;
        public Writer(String path) {
            this.path = path;
            this.shouldBreak = false;
        }

        public boolean shouldBreakOrNot() {
            return shouldBreak;
        }
        public void breakIt() {
            shouldBreak = true;
        }

        @Override
        public void run() {
            Runtime runtime = Runtime.getRuntime();
            String [] cmd= new String[3];
            cmd[0] = "/bin/sh";
            cmd[1] = "-c";
            cmd[2] = "echo `date` >> " + path;
            System.out.println(cmd[2]);
            try {
                while(true) {
                    Process process = runtime.exec(cmd);
                    StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR");
                    StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "OUTPUT");
                    errorGobbler.start();
                    outputGobbler.start();
                    int exitValue = process.waitFor();
//                    System.out.println("ExitValue:" + exitValue);
                    Thread.sleep(500);
                    if (shouldBreak) {
                        break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("The command is incorrect!");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
        
    static class StreamGobbler extends Thread {
        InputStream is;
        String type;
        StreamGobbler(InputStream is, String type) {
            this.is = is;
            this.type = type;
        }
        
        @Override
        public void run() {
            try {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line=null;
                while ( (line = br.readLine()) != null)
                System.out.println(type + ">" + line);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    static class MVandRollCommand implements Runnable {
        private SimTailServer server;
        private File file;
        public MVandRollCommand(File file, SimTailServer server) {
            this.file = file;
            this.server = server;
        }
        public void run() {
            assertNotNull(file);
            if (!file.exists()) {
                LOG.debug(file + " not found.");
                return;
            }
            try {
                Thread.sleep(3000);
                File des = new File(file.getParent(), MAGIC 
                        + new SimpleDateFormat("yyyy-MM-dd.hh").format(Util.getOneHoursAgoTime(new Date()))
                        + ".mv");
                file.renameTo(des);
                setBefore();
                Thread.sleep(3000);
                if (!server.shouldStopOrNot()) {
                    LOG.info("time is up, shutdown the server");
                    server.stopIt();
                }
                setAfter();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void test() {
//        System.out.println(System.getProperty("java.class.path"));
        LogReader reader = new LogReader(appnode, appLog, true);
        ExecutorService exec = Executors.newCachedThreadPool();
        exec.execute(reader);
        try {
            serverThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertNotNull("tailer function fail.", receives.toArray());
    }
    
    @Test
    public void testFileNotFoundAndFileRotated() {
        LogReader reader = new LogReader(appnode, appLog, true);
        ExecutorService exec = Executors.newCachedThreadPool();
        exec.execute(reader);
        ExecutorService exec2 = Executors.newSingleThreadExecutor();
        MVandRollCommand mvCommand = new MVandRollCommand(new File(appLog.getTailFile()), server);
        exec2.execute(mvCommand);
        try {
            serverThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertNotNull("testFileNotFound function fail.", receives.toArray());
        assertNotSame(getBefore(), getAfter());
    }
}

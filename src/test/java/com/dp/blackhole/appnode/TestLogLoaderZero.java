package com.dp.blackhole.appnode;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.appnode.TestLogReader.Server;

public class TestLogLoaderZero {
  private static final Log LOG = LogFactory.getLog(TestLogLoaderZero.class);
  private static final int offset = 100;
  private static final String MAGIC = "r34ff3r2013-01-01.15:00:15";
  private static final String rollIdent = MAGIC;
  private static File file;
  private static final int PORT = 40000;
  private static AppLog appLog;
  private static List<String> receives = new ArrayList<String>();
  private Server server;
  private Thread serverThread;
  private static final String expected = " 0f j2390jr092jf2f02jf02qjdf2-3j0 fiopwqejfjwffhg5_p  <end";
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String string = 
        "begin>  owefoq jfojnofownfowofnownefowoefojweofjwosfnvvoco\n" +
        "jlsdfpasjdfaopsdpfaskdfkpasdkpfkasdfas   100>   jcsopdnvon\n" +
        "vononoifjopwejf opwjfiop jpwj fopqwejfop qjfopiqjqertgbrtg\n" +
        "aspd jfoiasj df ioajsiodf asj fasof jasdopjf pasfj asopfjo\n" +
        "rtgrtghrthrthrthrhrthtrp sjfop asdj fopasj fopsfjopsjf wef\n" +
        "j faiosjf opwqejo fjopwej faeopsf jopawefj opsjf opsafj ao\n" +
        " wopejf opwj efopqwj epo fjwopefj pwef opw ejfopwj efopwf \n" +
        "3 wjopef joiqwf io j 9049 fj2490r 0pjfioj fioj qiowegio f \n" +
        " f90fj 9034u j90 jgioqpwejf iopwe jfopqwefj opewji fopq934\n" +
        expected + "\n";
    //build a app log
    file = File.createTempFile(MAGIC, null);
    LOG.info("create tmp file for test LogLoaderZero " + file);
    BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(file)));
    writer.write(string);
    writer.flush();
    writer.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    LOG.info("delete tmp file for test LogLoaderZero " + file);
    file.deleteOnExit();
  }

  @Before
  public void setUp() throws Exception {
    appLog = new AppLog(MAGIC, file.getAbsolutePath(), System.currentTimeMillis(), "localhost", PORT);
    server = new Server(PORT, receives);
    serverThread = new Thread(server);
    serverThread.start();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test() {
    LogLoaderZero loaderZ = new LogLoaderZero(appLog, rollIdent, offset);
    Thread thread = new Thread(loaderZ);
    thread.start();
    try {
      serverThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertEquals("loader function fail.", expected, receives.get(receives.size()-1));
  }

}

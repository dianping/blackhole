package com.dp.blackhole.appnode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLogReader {
  private static final Log LOG = LogFactory.getLog(TestLogReader.class);
  private static AppLog appLog;
  private static Appnode appnode;
  private static final String MAGIC = "sdfjiojwe";
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    //build a server
    new Thread(new Server(40000)).start();

    //build a app log
    File file = File.createTempFile(MAGIC, null);
    String fileAbsolutePath = file.getAbsolutePath();
    LOG.info(fileAbsolutePath);
    new Thread(new Write(file)).start();
    appLog = new AppLog(MAGIC, fileAbsolutePath, System.currentTimeMillis(), "127.0.0.1", 40000);

    //build a app node
    String appClient = "127.0.0.1";
    appnode = new Appnode(appClient);
  }



  @AfterClass
  public static void tearDownAfterClass() throws Exception {
//    appLog.getTailFile().deleteOnExit();
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  static class Write implements Runnable {
    private File file;
    OutputStreamWriter writer;
    public Write(File file) {
      this.file = file;
    }
    public void run() {
      try {
        int i = 0;
        writer = new OutputStreamWriter(new FileOutputStream(file));
        while(true) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          String line = "a message number " + i++;
          try {
            writer.write(line);
            writer.write('\n');
            writer.flush();
          } catch (IOException e) {
            e.printStackTrace();
          }
          System.out.println("writetofile>" + line);
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    }
  }
  
  static class Server implements Runnable {
    private ServerSocket ss;
    public Server(int port) {
      try {
        ss = new ServerSocket(port);
        System.out.println("server begin at " + port);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    public void run() {
      Socket socket = null;
      InputStream in = null;
      while (true) {
        System.out.println(Thread.currentThread());
        try {
          String line = null;
          socket = ss.accept();
          in = socket.getInputStream();
          BufferedReader reader = new BufferedReader(new InputStreamReader(in));
          while ((line = reader.readLine()) != null) {
            System.out.println("server>" + line);
          }
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          try {
            in.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }
  @Test
  public void test() {
    LogReader reader = new LogReader(appnode, appLog, false);
    ExecutorService exec = Executors.newCachedThreadPool();
    exec.execute(reader);
    while(true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testBase() {
    while(true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}

package com.dp.blackhole.appnode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.conf.BasicConfigurationConstants;
import com.dp.blackhole.conf.Configuration;

public class TestAppnode {
  private static final Log LOG = LogFactory.getLog(TestAppnode.class);
  private String client;
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    try {
      client = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e1) {
      LOG.error(e1);
      return;
    }
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testProcess() {
    fail("Not yet implemented");
  }

  @Test
  public void testLoadLocalConfig() throws ParseException, IOException {
    File confFile = File.createTempFile("app.conf", null);
    OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(confFile, true));
    writer.write("testApp.watchFile = /tmp/testApp.log\n");
    writer.write("testApp.second = sencond preperty\n");
    writer.flush();
    writer.close();
    
    String[] args = new String[2];
    args[0] = "-f";
    args[1] = confFile.getAbsolutePath();
    Appnode appnode = new Appnode(client);
    appnode.setArgs(args);
    assertTrue(appnode.parseOptions());
    appnode.loadLocalConfig();
    assertTrue(Configuration.appConfigMap.containsKey("testApp"));
    String path = Configuration.appConfigMap.get("testApp")
        .getString(BasicConfigurationConstants.WATCH_FILE);
    assertEquals(path, "/tmp/testApp.log");
    assertTrue(Configuration.appConfigMap.containsKey("testApp"));
    String second = Configuration.appConfigMap.get("testApp")
        .getString("second");
    assertEquals(second, "sencond preperty");
    confFile.deleteOnExit();
  }

  @Test
  public void testRun() {
    fail("Not yet implemented");
  }

}

package com.dp.blackhole.collectornode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.appnode.TestLogReader;

public class TestHDFSWriter {
  private static final Log LOG = LogFactory.getLog(TestHDFSWriter.class);
  private static final String MAGIC = "dcasdfef";
  private static final String TEST_PATH = "hdfs://10.1.77.86:/user/workcron/tmp/";
  private File file;
  private static FileSystem fs;
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);
    try {
        SecurityUtil.login(conf, "test.hadoop.keytab.file", "test.hadoop.principal");
      } catch (IOException e) {
        LOG.error("Faild to login with keytab security.");
        e.printStackTrace();
        throw e;
      }
    try {
      fs = FileSystem.get(conf);
      System.out.println(fs.getConf().get("fs.default.name"));
    } catch (IOException e) {
      LOG.error("Failed to get FileSystem.");
      e.printStackTrace();
      throw e;
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    //build a tmp file
    file = File.createTempFile(MAGIC, ".tmp");
    OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(file));
    writer.write("123456789012345678901234567890");
    writer.flush();
    String fileAbsolutePath = file.getAbsolutePath();
    LOG.info(fileAbsolutePath);
    writer.close();
  } 

  @After
  public void tearDown() throws Exception {
    List<File> candidateFiles = Arrays.asList(new File("/tmp").listFiles());
    for (File file : candidateFiles) {
      if (file.getName().startsWith(MAGIC)) {
        LOG.debug("delete tmp file " + file);
        file.deleteOnExit();
      }
    }
  }

  @Test
  public void test() {
    HDFSWriter writer = new HDFSWriter(fs, file, TEST_PATH + file.getName(), 0, 10);
    Thread thread = new Thread(writer);
    thread.start();
  }

}

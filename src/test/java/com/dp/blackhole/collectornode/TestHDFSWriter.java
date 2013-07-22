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
import com.dp.blackhole.testutil.TestUtil;

public class TestHDFSWriter {
  private static final Log LOG = LogFactory.getLog(TestHDFSWriter.class);
  private static final String MAGIC = "dcasdfef";
  private static final String TEST_PATH = "file:///tmp/";
  private static final String expected = " 0f j2390jr092jf2f02jf02qjdf2-3j0 fiopwqejfjwffhg5_p  <end";
  private static String md5sum;
  private File file;
  private static FileSystem fs;
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = new Configuration();
    try {
      fs = FileSystem.get(conf);
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
    file = TestUtil.createTmpFile(MAGIC, expected);
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
  public void test() throws InterruptedException {
    HDFSWriter writer = new HDFSWriter(fs, file, TEST_PATH + file.getName() + "_hdfs", false);
    Thread thread = new Thread(writer);
    thread.start();
    thread.join();
    //TODO md5sum
  }

}

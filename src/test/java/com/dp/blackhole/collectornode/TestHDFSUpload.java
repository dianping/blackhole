package com.dp.blackhole.collectornode;


import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.simutil.SimCollectornode;
import com.dp.blackhole.simutil.Util;

public class TestHDFSUpload {
    private static final Log LOG = LogFactory.getLog(TestHDFSUpload.class);
    private static final String MAGIC = "dcasdfef";
    private static final String HADOOP_PERFIX = "/tmp/hadoop.";
    private static File file;
    private static FileSystem fs;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        //build a tmp file
        file = Util.createTmpFile(MAGIC, Util.expected);
        
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
        LOG.info("delete tmp file for test HDFSWriter " + file);
        Util.deleteTmpFile(MAGIC);
    }

    @Before
    public void setUp() throws Exception {
    } 

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testUploadWhole() throws InterruptedException, IOException {
        HDFSUpload writer = new HDFSUpload(SimCollectornode.getSimpleInstance("upload", fs, MAGIC), fs, file, HADOOP_PERFIX + file.getName(), false);
        Thread thread = new Thread(writer);
        thread.start();
        thread.join();
        FileInputStream fis = new FileInputStream(file);
        String expectedMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
        Path actualFile = new Path(Util.SCHEMA + HADOOP_PERFIX + file.getName());
        String actualMDS = "";
        if (fs.exists(actualFile)) {
            actualMDS = org.apache.commons.codec.digest.DigestUtils.md5Hex(fs.open(actualFile));
        }
        assertEquals("md5sum not equals", expectedMD5, actualMDS);
        fis.close();
        fs.deleteOnExit(actualFile);
    }

    @Test
    public void testUploadPortion() throws InterruptedException, IOException {
        HDFSUpload writer = new HDFSUpload(SimCollectornode.getSimpleInstance("upload", fs, MAGIC), fs, file, HADOOP_PERFIX + file.getName(), false, 
                100, file.length() - 100);
        Thread thread = new Thread(writer);
        thread.start();
        thread.join();
        FileInputStream fis = new FileInputStream(file);
        String expectedMD5 = "4148bc964782b92b9410b75de54417c7";
        Path actualFile = new Path(Util.SCHEMA + HADOOP_PERFIX + file.getName());
        String actualMD5 = "";
        if (fs.exists(actualFile)) {
            actualMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fs.open(actualFile));
        }
        assertEquals("md5sum not equals", expectedMD5, actualMD5);
        fis.close();
        fs.deleteOnExit(actualFile);
    }
}

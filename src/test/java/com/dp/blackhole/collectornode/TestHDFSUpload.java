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
import org.junit.Before;
import org.junit.Test;

import com.dp.blackhole.simutil.SimCollectornode;
import com.dp.blackhole.simutil.Util;

public class TestHDFSUpload {
    private static final Log LOG = LogFactory.getLog(TestHDFSUpload.class);
    private final String APP_HOST = "localhost";
    private final String MAGIC = "dcasdfef";
    private File file;
    private FileSystem fs;

    @Before
    public void setUp() throws Exception {
        //build a tmp file
        file = Util.createTmpFile(MAGIC, Util.expected);
        File renameFile = new File(file.getParent(), APP_HOST + "@" + MAGIC + "_" + Util.FILE_SUFFIX);
        file.renameTo(renameFile);
        file = renameFile;
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            LOG.error("Failed to get FileSystem.", e);
            throw e;
        }
    } 

    @After
    public void tearDown() throws Exception {
        Util.deleteTmpFile(MAGIC);
        file.delete();
    }

    @Test
    public void testUploadWhole() throws InterruptedException, IOException {
        //TODO check how to upload hdfs base dir
        HDFSUpload writer = new HDFSUpload(SimCollectornode.getSimpleInstance("upload", fs, MAGIC), 
                fs, file, Util.getRollIdent(MAGIC));
        Thread thread = new Thread(writer);
        thread.start();
        thread.join();
        FileInputStream fis = new FileInputStream(file);
        String expectedMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
        Path actualFile = new Path(Util.SCHEMA + Util.BASE_PATH 
                + MAGIC + "/2013-01-01/15/" + APP_HOST + "@"
                + MAGIC + "_2013-01-01.15.gz");
        String actualMDS = "";
        if (fs.exists(actualFile)) {
            actualMDS = org.apache.commons.codec.digest.DigestUtils.md5Hex(fs.open(actualFile));
        }
        assertEquals("md5sum not equals", expectedMD5, actualMDS);
        fis.close();
        fs.delete(new Path(Util.SCHEMA + Util.BASE_PATH + MAGIC), true);
    }
}

package com.dp.blackhole.collectornode;


import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dp.blackhole.collectornode.HDFSUpload;

public class TestHDFSUpload {
    private static final Log LOG = LogFactory.getLog(TestHDFSUpload.class);
    private final String APP_HOST = "localhost";
    private final String MAGIC = "dcasdfef";
    private File file;
    private FileSystem fs;

    @Before
    public void setUp() throws Exception {
        //build a tmp file
        file = createTmpFile(MAGIC, SimCollectornode.expected);
        File renameFile = new File(file.getParent(), APP_HOST + "@" + MAGIC + "_" + SimCollectornode.FILE_SUFFIX);
        file.renameTo(renameFile);
        file = renameFile;
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            LOG.debug("Failed to get FileSystem.", e);
            throw e;
        }
    } 

    @After
    public void tearDown() throws Exception {
        SimCollectornode.deleteTmpFile(MAGIC);
        file.delete();
    }

    @Test
    public void testUploadWhole() throws InterruptedException, IOException {
        HDFSUpload writer = new HDFSUpload(SimCollectornode.getSimpleInstance("upload", MAGIC, fs), 
                fs, file, SimCollectornode.getRollIdent(MAGIC));
        Thread thread = new Thread(writer);
        thread.start();
        thread.join();
        FileInputStream fis = new FileInputStream(file);
        String expectedMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
        Path actualFile = new Path(SimCollectornode.SCHEMA + SimCollectornode.BASE_PATH 
                + MAGIC + "/2013-01-01/15/" + APP_HOST + "@"
                + MAGIC + "_2013-01-01.15.gz");
        String actualMDS = "";
        if (fs.exists(actualFile)) {
            actualMDS = org.apache.commons.codec.digest.DigestUtils.md5Hex(fs.open(actualFile));
        }
        assertEquals("md5sum not equals", expectedMD5, actualMDS);
        fis.close();
        fs.delete(new Path(SimCollectornode.SCHEMA + SimCollectornode.BASE_PATH + MAGIC), true);
    }
    
    public File createTmpFile(String MAGIC, String expected) 
            throws IOException, FileNotFoundException {
        String string = 
                "begin>    owefoq jfojnofownfowofnownefowoefojweofjwosfnvvoco\n" +
                "jlsdfpasjdfaopsdpfaskdfkpasdkpfkasdfas     100>     jcsopdnvon\n" +
                "vononoifjopwejf opwjfiop jpwj fopqwejfop qjfopiqjqertgbrtg\n" +
                "aspd jfoiasj df ioajsiodf asj fasof jasdopjf pasfj asopfjo\n" +
                "rtgrtghrthrthrthrhrthtrp sjfop asdj fopasj fopsfjopsjf wef\n" +
                "j faiosjf opwqejo fjopwej faeopsf jopawefj opsjf opsafj ao\n" +
                " wopejf opwj efopqwj epo fjwopefj pwef opw ejfopwj efopwf \n" +
                "3 wjopef joiqwf io j 9049 fj2490r 0pjfioj fioj qiowegio f \n" +
                " f90fj 9034u j90 jgioqpwejf iopwe jfopqwefj opewji fopq934\n" +
                expected + "\n";
        //build a app log
        File file = new File("/tmp/" + MAGIC);
        file.createNewFile();
        LOG.debug("create tmp file " + file);
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file)));
        writer.write(string);
        writer.close();
        return file;
    }
}

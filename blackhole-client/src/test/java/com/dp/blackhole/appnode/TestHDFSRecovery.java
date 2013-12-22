package com.dp.blackhole.appnode;
import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dp.blackhole.appnode.AppLog;
import com.dp.blackhole.appnode.RollRecovery;
import com.dp.blackhole.appnode.SimAppnode;
import com.dp.blackhole.collectornode.SimCollectornode;
import com.dp.blackhole.conf.ConfigKeeper;

public class TestHDFSRecovery {
    private static final Log LOG = LogFactory.getLog(TestHDFSRecovery.class);
    private final String MAGIC = "e9wjd83h";
    private final String APP_HOST = "localhost";
    private static final int port = 40004;
    private File file;
    private File fileBroken;
    private FileSystem fs;
    private Path oldPath;
    private SimAppnode appnode;
    private Thread serverThread;

    @Before
    public void setUp() throws Exception {
        //build a tmp file
        fileBroken = createBrokenTmpFile(MAGIC + "_broken_", SimAppnode.expected);
        convertToGZIP(fileBroken);
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            LOG.debug("Failed to get FileSystem.", e);
            throw e;
        }
        //file:///tmp/e9wjd83h/2013-01-01/15/localhost_e9wjd83h_2013-01-01.03  e9wjd83h is appname
        oldPath = new Path(SimAppnode.SCHEMA + SimAppnode.BASE_PATH 
                + MAGIC + "/2013-01-01/15/" + APP_HOST + "@"
                + MAGIC + "_2013-01-01.15.gz.tmp");
        LOG.debug("old path in hdfs is " + oldPath);
        fs.copyFromLocalFile(false, true, new Path(fileBroken.toURI()), oldPath);
        
        //create a good file and rename it for client
        file = SimAppnode.createTmpFile(MAGIC, SimAppnode.expected);
        SimAppnode.createTmpFile(MAGIC + "." + SimAppnode.FILE_SUFFIX, SimAppnode.expected);
        SimAppnode.createTmpFile(APP_HOST + "@" + MAGIC + "_" + SimAppnode.FILE_SUFFIX, SimAppnode.expected);
        
        appnode = new SimAppnode();
        
        //deploy some condition
        ConfigKeeper confKeeper = new ConfigKeeper();
        confKeeper.addRawProperty(MAGIC+".rollPeriod", "3600");
        confKeeper.addRawProperty(MAGIC + ".maxLineSize", "1024");
        
        serverThread = new Thread(
                new SimCollectornode("recovery", MAGIC, port, fs));
        serverThread.start();
    }

    @After
    public void tearDown() throws Exception {
//        serverThread.interrupt();
        fs.delete(new Path(SimAppnode.SCHEMA + SimAppnode.BASE_PATH), true);
        fileBroken.delete();
        file.delete();
        SimAppnode.deleteTmpFile(MAGIC);
    }

    @Test
    public void test() throws IOException, InterruptedException {
        AppLog appLog = new AppLog(MAGIC, file.getAbsolutePath(), new Date().getTime(), 1024);
        RollRecovery clientTask = new RollRecovery(appnode, SimAppnode.HOSTNAME, port, appLog, SimAppnode.rollTS);
        Thread clientThread = new Thread(clientTask);
        clientThread.run();
        convertToGZIP(file);
        String expectedMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(file));
        
//        LOG.info("expected md5 is " + expectedMD5);
        Path newPath = new Path(SimAppnode.SCHEMA + SimAppnode.BASE_PATH 
                + MAGIC + "/2013-01-01/15/" + APP_HOST + "@"
                + MAGIC + "_2013-01-01.15.gz");
        File fileGzip = new File(newPath.toUri());
//        fileGzip = Util.convertToNomal(fileGzip);
        Thread.sleep(1000); //wait for file convert to zip accomplished
        String actualMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(fileGzip));
//        LOG.info("actual md5 is " + actualMD5);
        assertEquals("recovery file is not correct.", expectedMD5, actualMD5);
    }
    
    public void convertToGZIP(File file) 
            throws FileNotFoundException, IOException {
        byte[] buf = new byte[8196];
        BufferedInputStream bin= new BufferedInputStream(new FileInputStream(file));
        File gzFile = new File(file.getAbsoluteFile() + ".tmp");
        GZIPOutputStream gout = new GZIPOutputStream(new FileOutputStream(gzFile));
        int len;
        while ((len = bin.read(buf)) != -1) {
            gout.write(buf, 0, len);
        }
        gout.close();
        bin.close();
        gzFile.renameTo(file.getAbsoluteFile());
        file = gzFile;
    }

    public File createBrokenTmpFile(String MAGIC, String expected) 
            throws IOException, FileNotFoundException {
        String string = 
                "begin>    owefoq jfojnofownfowofnownefowoefojweofjwosfnvvoco\n" +
                "jlsdfpasjdfaopsdpfaskdfkpasdkpfkasdfas     100>     jcsopdnvon\n" +
                "vononoifjopwejf opwjfiop jpwj fopqwejfop qjfopiqjqertgbrtg\n" +
                "aspd jfoiasj df ioajsiodf asj fasof jasdopjf pasfj asopfjo\n" +
                "rtgrtghrthrthrthrhrthtrp sjfop asdj fopasj fopsfjopsjf wef\n" +
                "j faiosjf opwqejo fjopwej faeopsf jopawefj opsjf opsafj ao\n" ;
        //build a app log
        File file = File.createTempFile(MAGIC, null);
        LOG.debug("create tmp broken file " + file);
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file)));
        writer.write(string);
        writer.close();
        return file;
    }
}

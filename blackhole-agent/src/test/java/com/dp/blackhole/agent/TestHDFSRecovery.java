package com.dp.blackhole.agent;
import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.Compressor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dp.blackhole.agent.SimAgent;
import com.dp.blackhole.agent.AgentMeta.TopicId;
import com.dp.blackhole.broker.Compression;
import com.dp.blackhole.broker.Compression.Algorithm;
import com.dp.blackhole.broker.SimBroker;
import com.dp.blackhole.common.Util;

public class TestHDFSRecovery {
    private static final Log LOG = LogFactory.getLog(TestHDFSRecovery.class);
    private final String MAGIC = "e9wjd83h";
    private String APP_HOST;
    private static final int port = 40004;
    private File file;
    private File fileBroken;
    private FileSystem fs;
    private Path oldPath;
    private SimAgent agent;
    private SimBroker collecotr;

    @Before
    public void setUp() throws Exception {
        APP_HOST = Util.getLocalHost();
        //build a tmp file
        fileBroken = createBrokenTmpFile(MAGIC + "_broken_", SimAgent.expected);
        convertToGZIP(fileBroken);
        try {
            fs = (new Path("/tmp")).getFileSystem(new Configuration());
        } catch (IOException e) {
            LOG.debug("Failed to get FileSystem.", e);
            throw e;
        }
        //file:///tmp/e9wjd83h/2013-01-01/15/localhost_e9wjd83h_2013-01-01.15  e9wjd83h is appname
        oldPath = new Path(SimAgent.SCHEMA + SimAgent.BASE_PATH 
                + MAGIC + "/2013-01-01/15/" + APP_HOST + "@"
                + MAGIC + "_2013-01-01.15.gz.tmp");
        LOG.debug("old path in hdfs is " + oldPath);
        fs.copyFromLocalFile(false, true, new Path(fileBroken.toURI()), oldPath);
        
        //create a good file and rename it for client
        file = SimAgent.createTmpFile(MAGIC, SimAgent.expected);
        SimAgent.createTmpFile(MAGIC + "." + SimAgent.FILE_SUFFIX, SimAgent.expected);
        SimAgent.createTmpFile(APP_HOST + "@" + MAGIC + "_" + SimAgent.FILE_SUFFIX, SimAgent.expected);
        
        agent = new SimAgent();
        
        collecotr = new SimBroker(port);
        collecotr.start();
    }

    @After
    public void tearDown() throws Exception {
//        serverThread.interrupt();
        fs.delete(new Path(SimAgent.SCHEMA + SimAgent.BASE_PATH), true);
        fileBroken.delete();
        file.delete();
        SimAgent.deleteTmpFile(MAGIC);
    }

    @Test
    public void test() throws IOException, InterruptedException {
        TopicId topicId = new TopicId(MAGIC, null);
        AgentMeta appLog = new AgentMeta(topicId, file.getAbsolutePath(), 3600, 3600, 1024, 1L, 5, 4096, 1024*1024, 1, -1);
        RollRecovery clientTask = new RollRecovery(agent, SimAgent.HOSTNAME, port, appLog, SimAgent.rollTS, false, true);
        clientTask.setStartWaitTime(0);
        Thread clientThread = new Thread(clientTask);
        clientThread.run();
        convertToGZIP(file);
        String expectedMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(file));
        
//        LOG.info("expected md5 is " + expectedMD5);
        Path newPath = new Path(SimAgent.SCHEMA + SimAgent.BASE_PATH 
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
        Algorithm compressionAlgo = Compression.getCompressionAlgorithmByName("gz");
        Compressor compressor = compressionAlgo.getCompressor();
        OutputStream gout = compressionAlgo.createCompressionStream(
                new FileOutputStream(gzFile), compressor, 0);
        int len;
        int count = 0;
        while ((len = bin.read(buf)) != -1) {
            gout.write(buf, 0, len);
            count += len;
        }
        System.out.println("count " + count);
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

package com.dp.blackhole.broker;


import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.Compressor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dp.blackhole.broker.HDFSUpload;
import com.dp.blackhole.broker.RollIdent;
import com.dp.blackhole.broker.RollManager;
import com.dp.blackhole.broker.Compression.Algorithm;
import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.StorageManager;
import com.dp.blackhole.broker.storage.RollPartition;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;

public class TestHDFSUpload {
    private final String MAGIC = "TestHDFSUpload_" + Util.getTS();
    private File expect;
    private FileSystem fs;
    private Algorithm compressionAlgo;

    @Before
    public void setUp() throws Exception {
        Properties prop = new Properties();
        prop.load(ClassLoader.getSystemResourceAsStream("config.properties"));
        String compressionAlgoName = prop.getProperty("broker.hdfs.compression.default");
        this.compressionAlgo = Compression.getCompressionAlgorithmByName(compressionAlgoName);
        //build a expect file
        expect = createExpectFile(getExpectFile()+"."+compressionAlgoName);
        Configuration conf = new Configuration();
        fs = FileSystem.get(conf);
    } 

    @After
    public void tearDown() throws Exception {
        File testdir = new File("/tmp/testHDFSUpload");
        if (testdir.exists()) {
            Util.rmr(testdir);
        }
        expect.delete();
        new File(getRealFile()+"."+compressionAlgo.getName()).delete();
    }

    @Test
    public void testUploadWhole() throws InterruptedException, IOException {
        RollIdent ident = getRollIdent(MAGIC);
        
        Partition p = createPartition();
        
        appendData(p);
        appendData(p);
        
        RollPartition roll1 = p.markRollPartition();
        
        appendData(p);
        
        RollManager mgr = mock(RollManager.class);
        when(mgr.getRollHdfsPath(ident, compressionAlgo.getName())).thenReturn(getRealFile()+"."+compressionAlgo.getName());
        StorageManager manager = mock(StorageManager.class);
        when(manager.getPartition(ident.topic, ident.source, false)).thenReturn(p);
        
        HDFSUpload writer = new HDFSUpload(mgr, manager, fs, ident, roll1, compressionAlgo.getName());
        Thread thread = new Thread(writer);
        thread.start();
        thread.join();
        
        FileInputStream fis = new FileInputStream(expect);
        String expectedMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
        String actualFile = getRealFile();
        String actualMDS = org.apache.commons.codec.digest.DigestUtils
                .md5Hex(new FileInputStream(actualFile + "."
                        + compressionAlgo.getName()));
        assertEquals("md5sum not equals", expectedMD5, actualMDS);
        fis.close();
    }
    
    public RollIdent getRollIdent(String appName) {
        RollIdent rollIdent = new RollIdent();
        rollIdent.topic = appName;
        rollIdent.period = 3600;
        rollIdent.source = SimBroker.HOSTNAME;
        rollIdent.ts = SimBroker.rollTS;
        return rollIdent;
    }
    
    public File createExpectFile(String filename) 
            throws IOException, FileNotFoundException {
        
        File file = new File(filename);
        Compressor compressor = compressionAlgo.getCompressor();
        OutputStream compressionOutput = compressionAlgo
                .createCompressionStream(new FileOutputStream(file), compressor, 0);
        
        for (int i=0; i < 65; i++) {
            compressionOutput.write(Integer.toString(i).getBytes());
            compressionOutput.write("\n".getBytes());
        }
        
        for (int i=0; i < 65; i++) {
            compressionOutput.write(Integer.toString(i).getBytes());
            compressionOutput.write("\n".getBytes());
        }
               
        compressionOutput.close();
        return file;
    }
    
    public String getExpectFile() {
        return "/tmp/expect_" + MAGIC;
    }
    
    public String getRealFile() {
        return "/tmp/real_" + MAGIC;
    }
    
    public Partition createPartition() throws IOException {
        File testdir = new File("/tmp/testHDFSUpload");
        if (testdir.exists()) {
            Util.rmr(testdir);
        }
        testdir.mkdirs();
        
        Partition partition = new Partition(testdir.getAbsolutePath(), "test", "localhost-1", 1024, 128);
        return partition;
    }
    
    public static void appendData(Partition p) throws IOException {
        ByteBuffer messageBuffer = ByteBuffer.allocate(2048);      
        for (int i=0; i < 65; i++) {
            Message message = new Message(Integer.toString(i).getBytes());
            message.write(messageBuffer);
        }
        
        messageBuffer.flip();
        ByteBufferMessageSet messages1 = new ByteBufferMessageSet(messageBuffer);       
        p.append(messages1);
    }
}

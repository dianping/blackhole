package com.dp.blackhole.collectornode;


import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dp.blackhole.collectornode.HDFSUpload;
import com.dp.blackhole.collectornode.persistent.Partition;
import com.dp.blackhole.collectornode.persistent.PersistentManager;
import com.dp.blackhole.collectornode.persistent.RollPartition;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;

public class TestHDFSUpload {
    private static final Log LOG = LogFactory.getLog(TestHDFSUpload.class);
    private final String MAGIC = "TestHDFSUpload_" + Util.getTS();
    private File expect;
    private FileSystem fs;

    @Before
    public void setUp() throws Exception {
        //build a expect file
        expect = createExpectFile(getExpectFile()+".gz");
        Configuration conf = new Configuration();
        fs = FileSystem.get(conf);
    } 

    @After
    public void tearDown() throws Exception {
//      File testdir = new File("/tmp/testHDFSUpload");
//        if (testdir.exists()) {
//            Util.rmr(testdir);
//        }
//        expect.delete();
//        new File(getRealFile()+".gz").delete();
    }

    @Test
    public void testUploadWhole() throws InterruptedException, IOException {
        RollIdent ident = getRollIdent(MAGIC);
        
        Partition p = createPartition();
        
        appendData(p);
        appendData(p);
        
        RollPartition roll1 = p.markRotate();
        
        appendData(p);
        
        RollManager mgr = mock(RollManager.class);
        when(mgr.getRollHdfsPath(ident)).thenReturn(getRealFile()+".gz");
        
        PersistentManager manager = mock(PersistentManager.class);
        when(manager.getPartition(ident.app, ident.source)).thenReturn(p);
        
        HDFSUpload writer = new HDFSUpload(mgr, manager, fs, ident, roll1);
        Thread thread = new Thread(writer);
        thread.start();
        thread.join();
        
        FileInputStream fis = new FileInputStream(expect);
        String expectedMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
        String actualFile = getRealFile();
        String actualMDS = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(actualFile+".gz"));
        assertEquals("md5sum not equals", expectedMD5, actualMDS);
        fis.close();
    }
    
    public RollIdent getRollIdent(String appName) {
        RollIdent rollIdent = new RollIdent();
        rollIdent.app = appName;
        rollIdent.period = 3600;
        rollIdent.source = SimCollectornode.HOSTNAME;
        rollIdent.ts = SimCollectornode.rollTS;
        return rollIdent;
    }
    
    public File createExpectFile(String filename) 
            throws IOException, FileNotFoundException {
        
        File file = new File(filename);
        GZIPOutputStream gout = new GZIPOutputStream(new FileOutputStream(file));
        
        for (int i=0; i < 65; i++) {
            gout.write(Integer.toString(i).getBytes());
            gout.write("\n".getBytes());
        }
        
        for (int i=0; i < 65; i++) {
            gout.write(Integer.toString(i).getBytes());
            gout.write("\n".getBytes());
        }
               
        gout.close();
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

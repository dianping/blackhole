package com.dp.blackhole.broker.ftp;

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
import java.util.zip.GZIPOutputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import com.dp.blackhole.broker.RollIdent;
import com.dp.blackhole.broker.RollManager;
import com.dp.blackhole.broker.SimBroker;
import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.RollPartition;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;

public class TestFTPUpload {
    private FakeFtpServer fakeFtpServer;
    private final String MAGIC = "TestFTPUpload_" + Util.getTS();
    private final String FTP_BASE_DIR = "/tmp/ftp";
    private File expect;
    
    @Before
    public void setUp() throws Exception {
        //build a expect file
        expect = createExpectFile(getExpectFile()+".gz");
        fakeFtpServer = new FakeFtpServer();
        fakeFtpServer.setServerControlPort(0);  // use any free port

        
        FileSystem fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new DirectoryEntry(FTP_BASE_DIR));
        fileSystem.add(new FileEntry(FTP_BASE_DIR + "/test", "test"));
        fakeFtpServer.setFileSystem(fileSystem);
        fakeFtpServer.addUserAccount(new UserAccount("foo", "bar", FTP_BASE_DIR));
        
        fakeFtpServer.start();
    }

    @After
    public void tearDown() throws Exception {
        fakeFtpServer.stop();
        expect.delete();
    }

    @Test
    public void testUploadWhole() throws IOException, InterruptedException {
        RollIdent ident = getRollIdent(MAGIC);
        
        Partition p = createPartition();
        
        appendData(p);
        appendData(p);
        
        RollPartition roll1 = p.markRotate();
        
        appendData(p);
        
        RollManager mgr = mock(RollManager.class);
        when(mgr.getParentPath(FTP_BASE_DIR, ident)).thenReturn("/tmp/ftp/" + MAGIC + "/2013-01-01/15");
        when(mgr.getCompressedFileName(ident)).thenReturn("localhost@" + MAGIC + "_2013-01-01.15.gz");
        
        FTPConfigration configration = new FTPConfigration("localhost", fakeFtpServer.getServerControlPort(), "foo", "bar", FTP_BASE_DIR);
        FTPUpload writer = new FTPUpload(mgr, configration, ident, roll1, p);
        Thread thread = new Thread(writer);
        thread.start();
        thread.join();
        
        FileInputStream fis = new FileInputStream(expect);
        String expectedMD5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
        
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect("localhost", fakeFtpServer.getServerControlPort());
        ftpClient.login("foo", "bar");
        
        File file = new File("/tmp/real_" + MAGIC + ".gz");
        OutputStream outputStream = new FileOutputStream(file);
        
        String actualFile = getRealFile();
        boolean success = ftpClient.retrieveFile(actualFile, outputStream);
        assertTrue(success);
        outputStream.close();
        ftpClient.disconnect();
        
        String actualMDS = org.apache.commons.codec.digest.DigestUtils.md5Hex(new FileInputStream(file));
        assertEquals("md5sum not equals", expectedMD5, actualMDS);
        fis.close();
        file.delete();
    }
    
    public RollIdent getRollIdent(String appName) {
        RollIdent rollIdent = new RollIdent();
        rollIdent.app = appName;
        rollIdent.period = 3600;
        rollIdent.source = SimBroker.HOSTNAME;
        rollIdent.ts = SimBroker.rollTS;
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
        return FTP_BASE_DIR + "/" + MAGIC + "/2013-01-01/15/localhost@" + MAGIC + "_2013-01-01.15.gz";
    }
    
    public Partition createPartition() throws IOException {
        File testdir = new File("/tmp/testFTPUpload");
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

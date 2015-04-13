package com.dp.blackhole.broker.storage;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Test;

import com.dp.blackhole.broker.SimBroker;
import com.dp.blackhole.broker.storage.Segment;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;

public class TestSegment {
    @After
    public void tearDown() throws Exception {
        SimBroker.deleteTmpFile("testSegment");
    }
    
    @Test
    public void test() throws IOException {
        File testdir = new File("/tmp/testSegment");
        if (testdir.exists()) {
            Util.rmr(testdir);
        }
        testdir.mkdirs();
        
        Segment segment = new Segment(testdir.getAbsolutePath(), 0, false, false, 1024, 108);
        
        ByteBuffer messageBuffer = ByteBuffer.allocate(2048);
        
        for (int i=0; i < 65; i++) {
            Message message = new Message("123".getBytes());
            message.write(messageBuffer);
        }
        messageBuffer.flip();
        ByteBufferMessageSet messages = new ByteBufferMessageSet(messageBuffer);     
        
        assertEquals(1040, segment.append(messages));
        assertEquals(0, segment.getStartOffset());
        assertEquals(1040, segment.getEndOffset());
        assertTrue(segment.contains(1024));
        assertFalse(segment.contains(1060));
        
        Segment reloadedsegment = new Segment(testdir.getAbsolutePath(), 0, true, true, 1024, 128);
        
        assertEquals(0, reloadedsegment.getStartOffset());
        assertEquals(1040, reloadedsegment.getEndOffset());
        assertTrue(reloadedsegment.contains(1024));
        assertFalse(reloadedsegment.contains(1060));
    }
    
    @Test
    public void testTruncate() throws IOException {
        File testdir = new File("/tmp/testSegment");
        if (testdir.exists()) {
            Util.rmr(testdir);
        }
        testdir.mkdirs();
        
        Segment segment = new Segment(testdir.getAbsolutePath(), 0, false, false, 1024, 128);
        
        ByteBuffer messageBuffer = ByteBuffer.allocate(2048);
        
        for (int i=0; i < 65; i++) {
            Message message = new Message("123".getBytes());
            message.write(messageBuffer);
        }
        messageBuffer.flip();
        messageBuffer.limit(messageBuffer.limit()-4);
        ByteBufferMessageSet messages = new ByteBufferMessageSet(messageBuffer);     
        
        assertEquals(1036, segment.append(messages));
        assertEquals(0, segment.getStartOffset());
        assertEquals(1036, segment.getEndOffset());
        assertTrue(segment.contains(1024));
        assertFalse(segment.contains(1060));
        
        Segment reloadedsegment = new Segment(testdir.getAbsolutePath(), 0, true, false, 1024, 128);
        
        assertEquals(0, reloadedsegment.getStartOffset());
        assertEquals(1024, reloadedsegment.getEndOffset());
        assertTrue(reloadedsegment.contains(1000));
        assertFalse(reloadedsegment.contains(1024));
        assertFalse(reloadedsegment.contains(1060));
    }
}

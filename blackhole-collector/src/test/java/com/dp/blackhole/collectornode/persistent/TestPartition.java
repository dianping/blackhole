package com.dp.blackhole.collectornode.persistent;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Test;

import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.Segment;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;

public class TestPartition {
    @Test
    public void test() throws IOException {
        File testdir = new File("/tmp/testPartition");
        if (testdir.exists()) {
            Util.rmr(testdir);
        }
        testdir.mkdirs();
        
        ByteBuffer messageBuffer = ByteBuffer.allocate(2048);      
        for (int i=0; i < 65; i++) {
            Message message = new Message("123".getBytes());
            message.write(messageBuffer);
        }
        Partition partition = new Partition(testdir.getAbsolutePath(), "test", "localhost-1", 1024, 128);
        
        messageBuffer.flip();
        ByteBufferMessageSet messages1 = new ByteBufferMessageSet(messageBuffer);       
        partition.append(messages1);
        
        messageBuffer.flip();
        ByteBufferMessageSet messages2 = new ByteBufferMessageSet(messageBuffer);          
        partition.append(messages2);
        
        messageBuffer.limit(150);
        messageBuffer.rewind();
        ByteBufferMessageSet messages3 = new ByteBufferMessageSet(messageBuffer);  
        partition.append(messages3);
        
        assertTrue(new File("/tmp/testPartition/test/localhost-1/0.blackhole").exists());
        assertEquals(1040, new File("/tmp/testPartition/test/localhost-1/0.blackhole").length());
        assertTrue(new File("/tmp/testPartition/test/localhost-1/1040.blackhole").exists());
        assertEquals(1040, new File("/tmp/testPartition/test/localhost-1/1040.blackhole").length());
        assertTrue(new File("/tmp/testPartition/test/localhost-1/2080.blackhole").exists());
        assertEquals(150, new File("/tmp/testPartition/test/localhost-1/2080.blackhole").length());
        
        Partition reloadedPartition = new Partition(testdir.getAbsolutePath(), "test", "localhost-1", 1024, 128);
        List<Segment> segments = reloadedPartition.getSegments();
        assertEquals(0, segments.get(0).getStartOffset());
        assertEquals(1040, segments.get(0).getEndOffset());
        assertEquals(1040, segments.get(1).getStartOffset());
        assertEquals(2080, segments.get(1).getEndOffset());
        assertEquals(2080, segments.get(2).getStartOffset());
        assertEquals(2224, segments.get(2).getEndOffset());
        
        assertEquals(segments.get(0), reloadedPartition.findSegment(0));
        assertEquals(segments.get(0), reloadedPartition.findSegment(1000));
        assertEquals(segments.get(1), reloadedPartition.findSegment(1040));
        assertEquals(segments.get(1), reloadedPartition.findSegment(2000));
        assertEquals(segments.get(2), reloadedPartition.findSegment(2090));
    }
}

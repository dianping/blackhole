package com.dp.blackhole.storage;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.junit.Test;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;
import com.dp.blackhole.storage.MessageAndOffset;

public class TestByteBufferMessageSet {
    @Test
    public void test() {
        ByteBuffer messageBuffer = ByteBuffer.allocate(2048);      
        for (int i=0; i < 65; i++) {
            Message message = new Message("123".getBytes());
            message.write(messageBuffer);
        }
        messageBuffer.flip();
        messageBuffer.limit(messageBuffer.limit() - 4);
        ByteBufferMessageSet messages = new ByteBufferMessageSet(messageBuffer, 0); 
        
        Iterator<MessageAndOffset> iter = messages.getItertor();
        int offset = 0;
        while (iter.hasNext()) {
            MessageAndOffset ms = iter.next();
            assertEquals("123", Message.toEvent(ms.message));
            assertEquals(offset, ms.offset);
            offset = offset + 16;
        }
        
    }
}

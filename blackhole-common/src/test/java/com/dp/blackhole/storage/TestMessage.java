package com.dp.blackhole.storage;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.junit.Test;

import com.dp.blackhole.storage.Message;

public class TestMessage {
    
    public static Charset charset = Charset.forName("UTF-8");
    public static CharsetEncoder encoder = charset.newEncoder();
    public static CharsetDecoder decoder = charset.newDecoder();
    
    @Test
    public void test() throws CharacterCodingException {
        String str = "123456789xyz";
        byte[] data = str.getBytes();
        Message message = new Message(data);
        
        ByteBuffer buf = ByteBuffer.allocate(1024);
        message.write(buf);
        buf.rewind();
        int size = buf.getInt();

        buf.limit(4 + size);
        Message m2 = new Message(buf.slice());
        assertEquals(str, decoder.decode(m2.payload()).toString());
        assertTrue(m2.valid());
    }
}

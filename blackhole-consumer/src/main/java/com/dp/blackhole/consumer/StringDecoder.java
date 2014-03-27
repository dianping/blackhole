package com.dp.blackhole.consumer;

import java.nio.ByteBuffer;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.storage.Message;

public class StringDecoder {
    public String toEvent(Message message) {
        ByteBuffer buf = message.payload();
        byte[] b = new byte[buf.remaining()];
        buf.get(b);
        return Util.fromBytes(b);
    }
}

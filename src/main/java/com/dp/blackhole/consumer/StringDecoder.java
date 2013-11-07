package com.dp.blackhole.consumer;

import java.nio.ByteBuffer;

import com.dp.blackhole.collectornode.persistent.Message;
import com.dp.blackhole.common.Util;

public class StringDecoder implements Decoder<String> {
    public String toEvent(Message message) {
        ByteBuffer buf = message.payload();
        byte[] b = new byte[buf.remaining()];
        buf.get(b);
        return Util.fromBytes(b);
    }
}

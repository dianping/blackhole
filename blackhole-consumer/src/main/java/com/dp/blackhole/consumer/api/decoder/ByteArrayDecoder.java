package com.dp.blackhole.consumer.api.decoder;

import java.nio.ByteBuffer;

import com.dp.blackhole.consumer.api.MessagePack;

public class ByteArrayDecoder implements Decoder<byte[]> {

    @Override
    public byte[] decode(MessagePack message) {
        ByteBuffer buf = message.payload();
        byte[] b = new byte[buf.remaining()];
        buf.get(b);
        return b;
    }

}

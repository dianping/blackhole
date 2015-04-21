package com.dp.blackhole.consumer.api.decoder;

import com.dp.blackhole.consumer.api.MessagePack;

public class StringDecoder implements Decoder<String> {
    
    @Override
    public String decode(MessagePack entity) {
        return entity.getContent();
    }
}

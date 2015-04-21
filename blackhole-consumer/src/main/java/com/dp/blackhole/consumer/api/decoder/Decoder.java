package com.dp.blackhole.consumer.api.decoder;

import com.dp.blackhole.consumer.api.MessagePack;

public interface Decoder<T> {

    T decode(MessagePack message);
}

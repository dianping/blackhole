package com.dp.blackhole.consumer;

import com.dp.blackhole.collectornode.persistent.Message;

public interface Decoder<T> {
    T toEvent(Message message);//TODO IMPORT
}

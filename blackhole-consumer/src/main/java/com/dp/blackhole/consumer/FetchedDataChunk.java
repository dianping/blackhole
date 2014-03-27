package com.dp.blackhole.consumer;

import com.dp.blackhole.storage.ByteBufferMessageSet;

public class FetchedDataChunk {
    public final ByteBufferMessageSet messages;

    public final PartitionTopicInfo topicInfo;

    public final long fetchOffset;

    public FetchedDataChunk(ByteBufferMessageSet messages, PartitionTopicInfo topicInfo, long fetchOffset) {
        this.messages = messages;
        this.topicInfo = topicInfo;
        this.fetchOffset = fetchOffset;
    }
}

package com.dp.blackhole.consumer;

import com.dp.blackhole.collectornode.persistent.ByteBufferMessageSet;

public class FetchedDataChunk {
    public final ByteBufferMessageSet messages;

    public final PartitionTopicInfo topicInfo;

    public final long fetchOffset;

    public FetchedDataChunk(ByteBufferMessageSet messages, PartitionTopicInfo topicInfo, long fetchOffset) {
        super();
        this.messages = messages;
        this.topicInfo = topicInfo;
        this.fetchOffset = fetchOffset;
    }
}

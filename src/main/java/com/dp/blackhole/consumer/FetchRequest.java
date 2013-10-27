package com.dp.blackhole.consumer;

import java.nio.ByteBuffer;


public class FetchRequest implements Request {

    /**
     * message topic
     */
    public final String topic;

    /**
     * partition of log file
     */
    public final int partition;

    /**
     * ofset of topic(log file)
     */
    public final long offset;

    /**
     * the max data size in bytes for this request
     */
    public final int maxSize;

    public FetchRequest(String topic, int partition, long offset) {
        this(topic, partition, offset, 64 * 1024);//64KB
    }
    /**
     * create a fetch request
     * 
     * @param topic the topic with messages
     * @param partition the partition of log file
     * @param offset offset of the topic(log file)
     * @param maxSize the max data size in bytes
     */
    public FetchRequest(String topic, int partition, long offset, int maxSize) {
        this.topic = topic;
        if (topic == null) {
            throw new IllegalArgumentException("no topic");
        }
        this.partition = partition;
        this.offset = offset;
        this.maxSize = maxSize;
    }

    public RequestKeys getRequestKey() {
        return RequestKeys.FETCH;
    }

    public int getSizeInBytes() {
        return Utils.caculateShortString(topic) + 4 + 8 + 4;
    }

    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxSize);
    }

    @Override
    public String toString() {
        return "FetchRequest(topic:" + topic + ", part:" + partition + " offset:" + offset + " maxSize:" + maxSize + ")";
    }

    /**
     * Read a fetch request from buffer(socket data)
     * 
     * @param buffer the buffer data
     * @return a fetch request
     * @throws IllegalArgumentException while error data format(no topic)
     */
    public static FetchRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int size = buffer.getInt();
        return new FetchRequest(topic, partition, offset, size);
    }

}

package com.dp.blackhole.collectornode.persistent.protocol;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class OffsetRequest extends NonDelegationTypedWrappable {
    public static final String SMALLES_TIME_STRING = "smallest";

    public static final String LARGEST_TIME_STRING = "largest";

    /**
     * reading the latest offset
     */
    public static final long LATES_TTIME = -1L;

    /**
     * reading the earilest offset
     */
    public static final long EARLIES_TTIME = -2L;

    ///////////////////////////////////////////////////////////////////////
    /**
     * message topic
     */
    public String topic;

    /**
     * topic partition,default value is 0
     */
    public String partition;

    /**
     * unix milliseconds time
     * LATES_TTIME: the latest(largest) offset</li>
     * EARLIES_TTIME: the earilest(smallest) offset</li>
     * time: the log file offset which lastmodified time earlier than the time
     */
    public long time;

    /**
     * number of offsets
     */
    public int maxNumOffsets;
    
    public OffsetRequest() {}
    /**
     * create a offset request
     * time:the log file created time
     * maxNumOffsets:the number of offsets
     */
    public OffsetRequest(String topic, String partition, long time, int maxNumOffsets) {
        this.topic = topic;
        this.partition = partition;
        this.time = time;
        this.maxNumOffsets = maxNumOffsets;
    }

    @Override
    public String toString() {
        return "OffsetRequest(topic:" + topic + ", part:" + partition + ", time:" + time + ", maxNumOffsets:" + maxNumOffsets + ")";
    }

    @Override
    public int getSize() {
        return GenUtil.getStringSize(topic) + GenUtil.getStringSize(partition) + 8 + 4;
    }

    @Override
    public void read(ByteBuffer buffer) {
        topic = GenUtil.readString(buffer);
        partition = GenUtil.readString(buffer);
        time = buffer.getLong();
        maxNumOffsets = buffer.getInt();
    }

    @Override
    public void write(ByteBuffer buffer) {
        GenUtil.writeString(topic, buffer);
        GenUtil.writeString(partition, buffer);
        buffer.putLong(time);
        buffer.putInt(maxNumOffsets);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.OffsetRequest;
    }
}

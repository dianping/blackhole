package com.dp.blackhole.protocol.data;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class OffsetRequest extends NonDelegationTypedWrappable {
    public static final String SMALLES_TIME_STRING = "smallest";

    public static final String LARGEST_TIME_STRING = "largest";

    /**
     * reading the latest offset
     */
    public static final long LATES_OFFSET = -1L;

    /**
     * reading the earilest offset
     */
    public static final long EARLIES_OFFSET = -2L;

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
     * autoOffset is one of the below
     * LATES_TTIME: the latest(largest) offset</li>
     * EARLIES_TTIME: the earilest(smallest) offset</li>
     */
    public long autoOffset;

    public OffsetRequest() {}
    /**
     * create a offset request
     * time:the log file created time
     * maxNumOffsets:the number of offsets
     */
    public OffsetRequest(String topic, String partition, long autoOffset) {
        this.topic = topic;
        this.partition = partition;
        this.autoOffset = autoOffset;
    }

    @Override
    public String toString() {
        return "OffsetRequest(topic:" + topic + ", part:" + partition + ", autoOffset:" + autoOffset + ")";
    }

    @Override
    public int getSize() {
        return GenUtil.getStringSize(topic) + GenUtil.getStringSize(partition) + 8;
    }

    @Override
    public void read(ByteBuffer buffer) {
        topic = GenUtil.readString(buffer);
        partition = GenUtil.readString(buffer);
        autoOffset = buffer.getLong();
    }

    @Override
    public void write(ByteBuffer buffer) {
        GenUtil.writeString(topic, buffer);
        GenUtil.writeString(partition, buffer);
        buffer.putLong(autoOffset);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.OffsetRequest;
    }
}

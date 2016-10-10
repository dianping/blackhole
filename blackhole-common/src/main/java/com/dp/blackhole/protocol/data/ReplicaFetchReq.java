package com.dp.blackhole.protocol.data;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class ReplicaFetchReq extends NonDelegationTypedWrappable {
    private int entropy;
    private String topic;
    private String partitionId;
    private String brokerLeader;
    private String brokerReplica;
    private long offset;
    private int limit;
    private long id;

    public ReplicaFetchReq() {

    }

    public ReplicaFetchReq(int entropy, String topic, String partitionId, String brokerLeader, String localhost,
            long offset, int limit, long id) {
        this.entropy = entropy;
        this.topic = topic;
        this.partitionId = partitionId;
        this.brokerLeader = brokerLeader;
        this.brokerReplica = localhost;
        this.offset = offset;
        this.limit = limit;
        this.id = id;
    }

    public int getEntropy() {
        return this.entropy;
    }

    public String getTopic() {
        return this.topic;
    }

    public String getPartition() {
        return this.partitionId;
    }

    public Long getOffset() {
        return this.offset;
    }

    public String getBrokerLeader() {
        return this.brokerLeader;
    }

    public String getBrokerReplica() {
        return this.brokerReplica;
    }

    public long getId() {
        return this.id;
    }

    @Override
    public int getSize() {
        return GenUtil.getStringSize(topic) + GenUtil.getStringSize(partitionId) + GenUtil.getStringSize(brokerLeader)
                + GenUtil.getStringSize(brokerReplica) + Long.SIZE * 2 / 8 + Integer.SIZE * 2 / 8;
    }

    @Override
    public void read(ByteBuffer buffer) {
        entropy = buffer.getInt();
        topic = GenUtil.readString(buffer);
        partitionId = GenUtil.readString(buffer);
        brokerLeader = GenUtil.readString(buffer);
        brokerReplica = GenUtil.readString(buffer);
        offset = buffer.getLong();
        limit = buffer.getInt();
        id = buffer.getLong();
    }

    @Override
    public void write(ByteBuffer buffer) {
        buffer.putInt(entropy);
        GenUtil.writeString(topic, buffer);
        GenUtil.writeString(partitionId, buffer);
        GenUtil.writeString(brokerLeader, buffer);
        GenUtil.writeString(brokerReplica, buffer);
        buffer.putLong(offset);
        buffer.putInt(limit);
        buffer.putLong(id);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.ReplicaFetchReq;
    }

    @Override
    public String toString() {
        return "fetchrequest entropy: " + entropy + ", " + topic + "/" + partitionId + ": " + offset + ", FetchId: " + this.id;
    }
}

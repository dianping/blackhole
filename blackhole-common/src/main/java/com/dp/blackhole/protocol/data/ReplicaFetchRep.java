package com.dp.blackhole.protocol.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import com.dp.blackhole.network.DelegationTypedWrappable;
import com.dp.blackhole.network.GenUtil;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.MessageSet;

public class ReplicaFetchRep extends DelegationTypedWrappable {

    private ByteBuffer headlength;
    private ByteBuffer head;
    private ByteBuffer messagesBuf;

    private int entropy;
    private String topic;
    private String partition;
    private String brokerLeader;
    private String brokerReplica;
    private MessageSet messages;
    private long offset;
    private long id;

    private int size;
    private int sent;

    private boolean complete;

    public ReplicaFetchRep() {
        headlength = ByteBuffer.allocate(4);
    }

    public ReplicaFetchRep(int entropy, String topic, String partition, String brokerLeader, String brokerReplica,
            long offset, MessageSet messages, long id) {
        this.entropy = entropy;
        this.topic = topic;
        this.partition = partition;
        this.brokerLeader = brokerLeader;
        this.brokerReplica = brokerReplica;
        this.offset = offset;
        this.messages = messages;
        if (messages != null) {
            this.size = messages.getSize();
        } else {
            this.size = 0;
        }
        this.id = id;

        this.headlength = ByteBuffer.allocate(4);
        headlength.putInt(getHeadSize());
        headlength.flip();

        this.head = ByteBuffer.allocate(getHeadSize());
        this.head.putInt(entropy);
        this.head.putInt(size);
        GenUtil.writeString(topic, this.head);
        GenUtil.writeString(partition, this.head);
        GenUtil.writeString(brokerLeader, this.head);
        GenUtil.writeString(brokerReplica, this.head);
        this.head.putLong(this.offset);
        this.head.putLong(id);
        this.head.flip();
    }

    private int getHeadSize() {
        return (Integer.SIZE * 2 + Long.SIZE * 2) / 8 + GenUtil.getStringSize(topic) + GenUtil.getStringSize(partition)
                + GenUtil.getStringSize(brokerLeader) + GenUtil.getStringSize(brokerReplica);
    }

    public MessageSet getMessageSet() {
        return this.messages;
    }

    public int getEntropy() {
        return this.entropy;
    }

    public String getTopic() {
        return this.topic;
    }

    public String getPartition() {
        return this.partition;
    }

    public String getBrokerLeader() {
        return this.brokerLeader;
    }

    public String getBrokerReplica() {
        return this.brokerReplica;
    }

    public long getOffset() {
        return this.offset;
    }

    public long getId() {
        return this.id;
    }

    @Override
    public final int getSize() {
        return size;
    }

    @Override
    public int read(ScatteringByteChannel channel) throws IOException {
        int read = 0;
        if (headlength.hasRemaining()) {
            int num = channel.read(headlength);
            if (num < 0) {
                throw new IOException("end-of-stream reached");
            }
            read += num;
            if (headlength.hasRemaining()) {
                return read;
            } else {
                headlength.flip();
                int headsize = headlength.getInt();
                head = ByteBuffer.allocate(headsize);
            }
        }
        if (head.hasRemaining()) {
            int num = channel.read(head);
            if (num < 0) {
                throw new IOException("end-of-stream reached");
            }
            read += num;
            if (head.hasRemaining()) {
                return read;
            } else {
                head.flip();
                entropy = head.getInt();
                size = head.getInt();
                topic = GenUtil.readString(head);
                partition = GenUtil.readString(head);
                brokerLeader = GenUtil.readString(head);
                brokerReplica = GenUtil.readString(head);
                offset = head.getLong();
                id = head.getLong();
                if (size != 0) {
                    messagesBuf = ByteBuffer.allocate(size);
                }
            }
        }
        if (!head.hasRemaining() && messagesBuf != null) {
            int num = channel.read(messagesBuf);
            if (num < 0) {
                throw new IOException("end-of-stream reached");
            }
            read += num;
            if (!messagesBuf.hasRemaining()) {
                messagesBuf.flip();
                messages = new ByteBufferMessageSet(messagesBuf, offset);
                complete = true;
            }
        } else if (messagesBuf == null) {
            complete = true;
        }
        return read;
    }

    @Override
    public int write(GatheringByteChannel channel) throws IOException {
        int written = 0;
        if (headlength.hasRemaining()) {
            written += GenUtil.retryWrite(channel, headlength);
            if (headlength.hasRemaining()) {
                return written;
            }
        }
        if (head.hasRemaining()) {
            written += GenUtil.retryWrite(channel, head);
            if (head.hasRemaining()) {
                return written;
            }
        }
        if (!head.hasRemaining() && (messages != null) && (size != 0)) {
            for (int i = 0; i < 16; i++) {
                int num = messages.write(channel, sent, size - sent);
                written += num;
                sent += num;
                if (num != 0) {
                    break;
                }
            }
        }

        if (sent >= size) {
            complete = true;
        }
        return written;
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.ReplicaFetchRep;
    }

    @Override
    public boolean complete() {
        return complete;
    }
}

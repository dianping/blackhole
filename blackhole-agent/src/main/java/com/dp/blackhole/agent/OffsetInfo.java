package com.dp.blackhole.agent;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.cat.Cat;

public class OffsetInfo {
    private static final Log LOG = LogFactory.getLog(OffsetInfo.class);

    private String topic;
    private String partition;
    private TreeMap<Long, ByteBuffer> msgMapping;
    private long currentOffset;

    public OffsetInfo(long currentOffset, String topic, String partition) {
        this.currentOffset = currentOffset;
        this.msgMapping = new TreeMap<Long, ByteBuffer>();
        this.topic = topic;
        this.partition = partition;
    }

    public void setCurrentOffset(long offset) {
        this.currentOffset = offset;
    }

    public long getCurrentOffset() {
        return this.currentOffset;
    }

    public long getMsgQueueSize() {
        return msgMapping.size();
    }

    public TreeMap<Long, ByteBuffer> initOffset(long offset) {
        ArrayList<Long> offsetQueue = new ArrayList<Long>(msgMapping.keySet());
        offsetQueue.add(this.currentOffset);
        LOG.debug("handle init message of start offset: " + offset + "\n" + this.toString());
        if (offset == offsetQueue.get(offsetQueue.size() - 1)) {
            return new TreeMap<Long, ByteBuffer>();
        }
        long endOffset = locateOffset(offset, offsetQueue);
        if (endOffset != -1L) {
            TreeMap<Long, ByteBuffer> readyToSend = new TreeMap<Long, ByteBuffer>();
            for (long o : msgMapping.keySet()) {
                if (o == endOffset) {
                    readyToSend.put(offset, copyToCeil(offset, o));
                } else if (o > endOffset) {
                    readyToSend.put(o, copy(o));
                }
            }
            return readyToSend;
        } else {
            // brokerHA debug metric
            Cat.logEvent("Agent-fail to find init offset, topic: " + this.topic, partition);
            LOG.fatal("fail to init offset, topic: " + topic + ", partition: " + partition + "\n" + this.toString()
                    + "\n" + "initOffset: " + offset + "\n");
            return null;
        }
    }

    public void newMsg(ByteBuffer byteBuffer, long validSize) {
        ByteBuffer bbCopy = copy(byteBuffer);
        msgMapping.put(this.currentOffset, bbCopy);
        currentOffset += validSize;
    }

    public boolean msgAck(long offset) {
        LOG.debug("handle message ACK of offset: " + offset + "\n" + this.toString());
        if (offset == currentOffset) {
            msgMapping.clear();
            return true;
        }
        if (offset > currentOffset) {
            // brokerHA debug metric
            Cat.logEvent("Agent fail to locate ack offset, topic: " + this.topic, this.partition);
            LOG.fatal("fail to locate ack offset, topic: " + topic + ", partition: " + partition + "\n"
                    + this.toString() + "\n" + "ackOffset: " + offset + "\n");
            return false;
        }
        ArrayList<Long> offsetQueue = new ArrayList<Long>(msgMapping.keySet());
        offsetQueue.add(currentOffset);
        long endOffset = locateOffset(offset, offsetQueue);
        if (endOffset != -1L) {
            for (long o : offsetQueue) {
                if (o < endOffset) {
                    msgMapping.remove(o);
                }
            }
            return true;
        } else {
            // brokerHA debug metric
            Cat.logEvent("Agent fail to locate ack offset, topic: " + this.topic, this.partition);
            LOG.fatal("fail to locate ack offset, topic: " + topic + ", partition: " + partition + "\n"
                    + this.toString() + "\n" + "ackOffset: " + offset + "\n");
            return false;
        }
    }

    public ByteBuffer getMsg(long offset) {
        return copy(offset);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("OffsetInfo: ");
        sb.append("{\n");
        sb.append("  ").append("OffsetQueue: " + msgMapping.keySet()).append("\n");
        sb.append("  ").append("CurrentOffset: " + this.currentOffset).append("\n");
        sb.append("}");
        return sb.toString();
    }

    private long locateOffset(long offset, ArrayList<Long> offsetQueue) {
        int low = 0;
        int high = offsetQueue.size() - 1;
        if (!containsOffset(offset, offsetQueue.get(low), offsetQueue.get(high))) {
            return -1L;
        }
        while (low < high) {
            int mid = (low + high) / 2;
            if (mid != low && mid != high) {
                if (containsOffset(offset, offsetQueue.get(low), offsetQueue.get(mid))) {
                    high = mid;
                } else if (containsOffset(offset, offsetQueue.get(mid), offsetQueue.get(high))) {
                    low = mid;
                }
            } else {
                break;
            }
        }
        return offsetQueue.get(low);
    }

    private boolean containsOffset(long offset, long low, long high) {
        if (offset < low || offset >= high) {
            return false;
        }
        return true;
    }

    private ByteBuffer copy(ByteBuffer origin) {
        ByteBuffer copy = origin.duplicate();
        return copy;
    }

    private ByteBuffer copy(long offset) {
        ByteBuffer origin = msgMapping.get(offset);
        return copy(origin);
    }

    private ByteBuffer copyToCeil(long offset, long floor) {
        ByteBuffer copy = copy(floor);
        while (true) {
            if (copy.remaining() < 4) {
                return null;
            }
            int length = copy.getInt();
            if (copy.remaining() < length) {
                return null;
            }
            if (offset == floor) {
                copy.position(copy.position() - 4);
                return copy;
            }
            if (offset < floor + length) {
                // brokerHA debug metric
                Cat.logEvent("Agent faile to init re-send, topic: " + this.topic, this.partition);
                LOG.fatal("Agent fail to init re-send, topic: " + this.topic + ", partition: " + this.partition + "\n"
                        + this.toString() + "\n" + "offset: " + offset + "\n" + "floor: " + floor + "\n" + "length: "
                        + length);
                copy.position(copy.position() - 4);
                return copy;
            }
            floor += (4 + length);
            copy.position(copy.position() + length);
        }
    }

}

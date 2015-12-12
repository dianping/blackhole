package com.dp.blackhole.protocol.data;

import java.nio.ByteBuffer;

import com.dp.blackhole.network.NonDelegationTypedWrappable;

public class ProducerRegReply extends NonDelegationTypedWrappable {

    private short flag;
    
    public ProducerRegReply() {
    }
    
    public ProducerRegReply(Boolean success) {
        if (success) {
            this.flag = 1;
        } else {
            this.flag = 0;
        }
    }
    
    public Boolean getResult() {
        if (flag == 1) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int getSize() {
        return Short.SIZE/8;
    }

    @Override
    public void read(ByteBuffer buffer) {
        flag = buffer.getShort();
    }

    @Override
    public void write(ByteBuffer buffer) {
        buffer.putShort(flag);
    }

    @Override
    public int getType() {
        return DataMessageTypeFactory.ProducerRegReply;
    }
}

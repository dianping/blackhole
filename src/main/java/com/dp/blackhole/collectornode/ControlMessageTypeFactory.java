package com.dp.blackhole.collectornode;

import com.dp.blackhole.common.PBmsg;
import com.dp.blackhole.network.TypedFactory;
import com.dp.blackhole.network.TypedWrappable;

public class ControlMessageTypeFactory implements TypedFactory {

    @Override
    public TypedWrappable getWrappedInstanceFromType(int type) {
        // TODO Auto-generated method stub
        return new PBmsg();
    }

}

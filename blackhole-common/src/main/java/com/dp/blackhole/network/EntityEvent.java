package com.dp.blackhole.network;

class EntityEvent<Entity, Connection extends NonblockingConnection<Entity>> {
    static final int EXPECTNONE = -1;
    static final int CONNECTED = 1;
    static final int DISCONNECTED = 2;
    static final int RECEIVED = 3;
    static final int RECEIVE_TIMEOUT = 4;
    static final int SEND_FALURE = 5;
    int type;
    Entity entity;
    int expect;
    Connection c;
    String callId;
    public EntityEvent(int type, Entity entity, Connection c) {
        this.type = type;
        this.entity = entity;
        this.expect = EXPECTNONE;
        this.c = c;
    }
    public EntityEvent(int type, Entity entity, int expect, Connection c, String callId) {
        this.type = type;
        this.entity = entity;
        this.expect = expect;
        this.c = c;
        this.callId = callId;
    }
}

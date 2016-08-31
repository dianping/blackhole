package com.dp.blackhole.network;

import java.util.concurrent.TimeUnit;

public interface NioService <Entity, Connection extends NonblockingConnection<Entity>> {
    public boolean running();
    
    public EntityProcessor<Entity, Connection> getProcessor();
    
    public Handler<Entity, Connection> getHandler(Connection conn);
    
    public void send(Connection conn, Entity msg);
    
    public void sendWithExpect(Connection conn, Entity event, int expect, int timeout, TimeUnit unit);
    
    public EntityEvent<Entity, Connection> unwatch(Connection conn, int expect);
    
    void shutdown();
}

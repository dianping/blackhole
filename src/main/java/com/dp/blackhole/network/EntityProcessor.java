package com.dp.blackhole.network;

public interface EntityProcessor<Entity, Connection extends NonblockingConnection<Entity>> {
    public void OnConnected(Connection connection);
    public void OnDisconnected(Connection connection);
    public void process(Entity reply, Connection from);
}

package com.dp.blackhole.network;

public interface EntityProcessor<Entity, Connection extends NonblockingConnection<Entity>> {
    public void OnConnected(Connection conn);
    public void OnDisconnected(Connection conn);
    public void process(Entity msg, Connection conn);
    public void receiveTimout(Entity msg, Connection conn);
    public void sendFailure(Entity msg, Connection conn);
    public void setNioService(NioService<Entity, Connection> service);
}

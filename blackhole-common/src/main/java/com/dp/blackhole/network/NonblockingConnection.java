package com.dp.blackhole.network;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.Lock;

public interface NonblockingConnection<Entity> {

    public boolean isActive();

    public SocketChannel getChannel();

    public SelectionKey keyFor(Selector selector);

    public int read() throws IOException;

    public boolean readComplete();

    public void readyforRead();

    public int write() throws IOException;

    public boolean writeComplete();

    public Entity getEntity();

    public void close();

    void send(Entity entity);

    public boolean isResolved();

    public Lock getLock();
}

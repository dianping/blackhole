package com.dp.blackhole.network;

import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public interface NonblockingConnectionFactory<Connection extends NonblockingConnection> {
    Connection makeConnection(SocketChannel channel, Selector selector, TypedFactory wrappedFactory);
}

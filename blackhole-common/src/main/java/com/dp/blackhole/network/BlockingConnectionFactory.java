package com.dp.blackhole.network;

import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public interface BlockingConnectionFactory<Connection extends BlockingConnection> {
    Connection makeConnection(SocketChannel channel, TypedFactory wrappedFactory);
}

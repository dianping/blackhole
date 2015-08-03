package com.dp.blackhole.network;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface BlockingConnection<Entity> {
    
    public boolean isActive();
    
    public SocketChannel getChannel();
    
    public void close();

    public Entity read() throws IOException;
    
    public int write(Entity entity) throws IOException;
    
    public boolean isResolved();
}

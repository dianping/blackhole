package com.dp.blackhole.common;

import java.io.IOException;

public interface StreamConnection {
    
    public String getSource();
    
    public String getTarget();
    
    public void sendMessage() throws IOException;
    
    public void sendSignal() throws IOException;
    
    public boolean canSend();
    
    public void close();
}

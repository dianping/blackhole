package com.dp.blackhole.common;

import java.io.IOException;

public interface Sender {
    
    public boolean isActive();
    
    public String getSource();
    
    public String getTarget();
    
    public void sendMessage() throws IOException;
    
    public void heartbeat() throws IOException;
    
    public boolean canSend();
    
    public void close();
}

package com.dp.blackhole.collectornode;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class client {

    public static void main(String args[]) throws IOException, IOException {
        Socket s = new Socket("localhost", 8081);
        
        PrintWriter w = new PrintWriter(s.getOutputStream());
        
        w.print("\n");
        
        w.close();
        s.close();
    }
}

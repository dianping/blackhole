package com.dp.blackhole.simutil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SimTailServer implements Runnable {
    private static final Log LOG = LogFactory.getLog(SimTailServer.class);
    
    private static final int MAX_SIZE = 10;
    private ServerSocket ss;
    private List<String> receives;
    private volatile boolean shouldStop;
    public SimTailServer(int port, List<String> receives) {
        this.shouldStop = false;
        this.receives = receives;
        try {
            ss = new ServerSocket(port);
            System.out.println("server begin at " + port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public boolean shouldStopOrNot() {
        return shouldStop;
    }
    public void stopIt() {
        shouldStop = true;
    }
    
    public void run() {
        Socket socket = null;
        InputStream in = null;
        System.out.println(Thread.currentThread());
        try {
            String line = null;
            socket = ss.accept();
            in = socket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            while (!shouldStop && (line = reader.readLine()) != null) {
                LOG.debug("server>" + line);
                receives.add(line);
                if (receives.size() == MAX_SIZE) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

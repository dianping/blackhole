package com.dp.blackhole.testutil;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RecoveryServer implements Runnable {
    private static final Log LOG = LogFactory.getLog(RecoveryServer.class);
    
    private ServerSocket ss;
    private List<String> header;
    private List<String> receives;
    private volatile boolean shouldStop;
    public RecoveryServer(int port, List<String> header, List<String> receives) {
        this.shouldStop = false;
        this.header = header;
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
        DataInputStream din = null;
        DataOutputStream dout = null;
        System.out.println(Thread.currentThread());
        try {
            String line = null;
            socket = ss.accept();
            in = socket.getInputStream();
            //check header
            din = new DataInputStream(in);
            String type = com.dp.blackhole.common.Util.readString(din);
            LOG.info("Receive... " + type);
            header.add(type);
            String appname = com.dp.blackhole.common.Util.readString(din);
            LOG.info("Receive... " + appname);
            header.add(appname);
            String periodStr = din.readLong() + "";
            LOG.info("Receive... " + periodStr);
            header.add(periodStr);
            String format = com.dp.blackhole.common.Util.readString(din);
            LOG.info("Receive... " + format);
            header.add(format); 
            
            //similar get offset from hdfs file
            long offset = getOffsetFromHDFSFile();
            //send the offset to client
            dout = new DataOutputStream(socket.getOutputStream());
            dout.writeLong(offset);
            System.out.println("server "+socket.isClosed());
            System.out.println("server "+socket.isConnected());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            while (!shouldStop && (line = reader.readLine()) != null) {
                LOG.info("server>" + line);
                receives.add(line);
                if (receives.size() == Util.MAX_LINE) {
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

    private long getOffsetFromHDFSFile() {
        return 100l;
    }
}

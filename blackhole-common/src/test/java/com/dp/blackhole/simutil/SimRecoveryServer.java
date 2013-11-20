package com.dp.blackhole.simutil;

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

import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.AgentProtocol.AgentHead;

public class SimRecoveryServer implements Runnable {
    private static final Log LOG = LogFactory.getLog(SimRecoveryServer.class);
    public static final int MAX_LINE = 9;
    private ServerSocket ss;
    private List<String> header;
    private List<String> receives;
    private volatile boolean shouldStop;
    public SimRecoveryServer(int port, List<String> header, List<String> receives) {
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
        try {
            if (!ss.isClosed()) {
                ss.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            AgentProtocol protocol = new AgentProtocol();
            AgentHead head = protocol.new AgentHead();
            
            protocol.recieveHead(din, head);
            String type = String.valueOf(head.type);
            LOG.debug("Receive... " + type);
            header.add(type);
            String appname = head.app;
            LOG.debug("Receive... " + appname);
            header.add(appname);
            String periodStr = String.valueOf(head.peroid);
            LOG.debug("Receive... " + periodStr);
            header.add(periodStr);
            String ts = String.valueOf(head.ts);
            LOG.debug("Receive... " + ts);
            header.add(ts); 
            
            //similar get offset from hdfs file
            long offset = getOffsetFromHDFSFile();
            //send the offset to client
            dout = new DataOutputStream(socket.getOutputStream());
            dout.writeLong(offset);
            System.out.println("server "+socket.isClosed());
            System.out.println("server "+socket.isConnected());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            while (!Thread.interrupted() && !shouldStop && (line = reader.readLine()) != null) {
                LOG.debug("server>" + line);
                receives.add(line);
                if (receives.size() == MAX_LINE) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (!ss.isClosed()) {
                    ss.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private long getOffsetFromHDFSFile() {
        return 100l;
    }
}

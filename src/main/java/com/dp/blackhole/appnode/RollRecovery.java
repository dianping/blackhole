package com.dp.blackhole.appnode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;

public class RollRecovery implements Runnable{
    private static final Log LOG = LogFactory.getLog(RollRecovery.class);
    private static final String RAF_MODE = "r";
    private static final int DEFAULT_BUFSIZE = 8192;
    private String collectorServer;
    private int port;
    private AppLog appLog;
    private long rollTimestamp;
    private Socket server;
    private byte[] inbuf;
    public RollRecovery(String collectorServer, int port, AppLog appLog, long rollTimestamp) {
        this.collectorServer = collectorServer;
        this.port = port;
        this.appLog = appLog;
        this.rollTimestamp = rollTimestamp;
        this.inbuf = new byte[DEFAULT_BUFSIZE];
    }
    
    @Override
    public void run() {
        long period = ConfigKeeper.configMap.get(appLog.getAppName())
                .getLong(ParamsKey.Appconf.ROLL_PERIOD, 3600l);
        SimpleDateFormat unitFormat = new SimpleDateFormat(Util.getFormatFromPeroid(period));
        String rollIdent = unitFormat.format(rollTimestamp);
        File rolledFile = Util.findRealFileByIdent(appLog.getTailFile(), rollIdent);
        RandomAccessFile reader = null;
        DataOutputStream out = null;
        DataInputStream in = null;
        long offset = 0;
        try {
            server = new Socket(collectorServer, port);
            out = new DataOutputStream(server.getOutputStream());
            in = new DataInputStream(server.getInputStream());
            
            AgentProtocol protocol = new AgentProtocol();
            AgentHead head = protocol.new AgentHead();
            head.type = AgentProtocol.RECOVERY;
            head.app = appLog.getAppName();
            head.peroid = period;
            head.ts = rollTimestamp;
            protocol.sendHead(out, head);
            offset = protocol.receiveOffset(in);
            LOG.info("Received offset [" + offset + "] in header response.");
            LOG.debug("roll file is " + rolledFile);
            reader = new RandomAccessFile(rolledFile, RAF_MODE);
            
            reader.seek(offset);
            LOG.info("Seeked to the position " + offset + ". Begin to transfer...");
            int len = 0;
            long transferBytes = 0;
            LOG.debug("server socket in client " + server.isClosed());
            while ((len = reader.read(inbuf)) != -1) {
                out.write(inbuf, 0, len);
                transferBytes += len;
            }
            out.flush();
            LOG.info("Roll file transfered, including [" + transferBytes + "] bytes.");
        } catch (FileNotFoundException e) {
            LOG.error("Oops, got an exception:", e);
        } catch (UnknownHostException e) {
            LOG.error("Faild to build a socket with host:" 
                    + collectorServer + " port:" + port, e);
        } catch (IOException e) {
            LOG.error("Faild to build Input/Output stream. ", e);
        } catch (Exception e) {
            LOG.error("Oops, got an exception:", e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                LOG.warn("Oops, got an exception:", e);
            }
        }
    }
}

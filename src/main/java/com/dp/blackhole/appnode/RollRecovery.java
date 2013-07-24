package com.dp.blackhole.appnode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.conf.AppConfigurationConstants;
import com.dp.blackhole.conf.Configuration;
import com.dp.blackhole.common.Util;

public class RollRecovery implements Runnable{
    private static final Log LOG = LogFactory.getLog(RollRecovery.class);
    private static final String RAF_MODE = "r";
    private static final int DEFAULT_BUFSIZE = 8102;
    private AppLog appLog;
//    private Appnode appnode;    //to persist it for next version
    private String rollIdent;
    private Socket server;
    private byte[] inbuf;
    public RollRecovery(AppLog appLog, String rollIdent) {
//        this.appnode = appnode;
        this.appLog = appLog;
        this.rollIdent = rollIdent;
        this.inbuf = new byte[DEFAULT_BUFSIZE];
    }
    
    @Override
    public void run() {
        File rolledFile = Util.findRealFileByIdent(appLog, rollIdent);
        RandomAccessFile reader = null;
        DataOutputStream out = null;
        DataInputStream in = null;
        long offset = 0;
        try {
            server = new Socket(appLog.getServer(), appLog.getPort());
            out = sendHeaderReq();
            
            offset = receiveHeaderRes(in);
            LOG.info("Receive a response including a offset " + offset);
            
            reader = new RandomAccessFile(rolledFile, RAF_MODE);
//            long length = rolledFile.length();
            reader.seek(offset);
            LOG.info("Seek to the position " + offset + " ok. Begin to transfer...");
            int len = 0;
            LOG.debug("server socket in client " + server.isClosed());
            while ((len = reader.read(inbuf)) != -1) {
                out.write(inbuf, 0, len);
                out.flush();
            }
            LOG.info("Roll file " + rolledFile + " has been transfered");
        } catch (FileNotFoundException e) {
            LOG.error("Oops, got an exception:", e);
        } catch (UnknownHostException e) {
            LOG.error("Faild to build a socket with host:" 
                    + appLog.getServer() + " port:" + appLog.getPort(), e);
        } catch (IOException e) {
            LOG.error("Faild to build Input/Output stream. ", e);
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

    /**
     * 1.specify a message type
     * 2.specify app name to collector
     * 3.specify a peroid
     * 4.specify a format
     * @return
     * @throws IOException
     */
    private DataOutputStream sendHeaderReq() throws IOException {
        DataOutputStream out;
        out = new DataOutputStream(server.getOutputStream());
        LOG.info("Writing... type]:recovery");
        Util.writeString("recovery", out);
        
        String appname = appLog.getAppName();
        LOG.info("Writing... appname:" + appname);
        Util.writeString(appname, out);
        
        String unit = Configuration.configMap.get(appLog.getAppName())
                .getString(AppConfigurationConstants.TRANSFER_PERIOD_UNIT, "hour");
        int value = Configuration.configMap.get(appLog.getAppName())
                .getInteger(AppConfigurationConstants.TRANSFER_PERIOD_VALUE, 1);
        long period = Util.getPeriodInSeconds(value, unit);
        LOG.info("Writing... period:" + period);
        out.writeLong(period);
        
        String format = Util.getFormatByUnit(unit);
        LOG.info("Writing... format:" + format);
        Util.writeString(format, out);
        out.flush();
        return out;
    }

    private long receiveHeaderRes(DataInputStream in) throws IOException {
        in = new DataInputStream(server.getInputStream());
        long offset = in.readLong();
        return offset;
    }
}

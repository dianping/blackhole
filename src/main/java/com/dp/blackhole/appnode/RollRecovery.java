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

import com.dp.blackhole.conf.AppConfigurationConstants;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.common.Util;

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
        String unit = ConfigKeeper.configMap.get(appLog.getAppName())
                .getString(AppConfigurationConstants.TRANSFER_PERIOD_UNIT, "hour");
        SimpleDateFormat unitFormat = new SimpleDateFormat(Util.getFormatByUnit(unit));
        String rollIdent = unitFormat.format(rollTimestamp);
        File rolledFile = Util.findRealFileByIdent(appLog.getTailFile(), rollIdent);
        RandomAccessFile reader = null;
        DataOutputStream out = null;
        DataInputStream in = null;
        long offset = 0;
        try {
            server = new Socket(collectorServer, port);
            out = sendHeaderReq();
            
            offset = receiveHeaderRes(in);
            LOG.info("Received offset [" + offset + "] in header response.");
            
            reader = new RandomAccessFile(rolledFile, RAF_MODE);
//            long length = rolledFile.length();
            reader.seek(offset);
            LOG.info("Seeked to the position " + offset + ". Begin to transfer...");
            int len = 0;
            long transferBytes = 0;
            LOG.debug("server socket in client " + server.isClosed());
            while ((len = reader.read(inbuf)) != -1) {
                out.write(inbuf, 0, len);
                transferBytes += len;
            }
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
        LOG.info("Writing... type:recovery");
        Util.writeString("recovery", out);
        
        String appname = appLog.getAppName();
        LOG.info("Writing... appname:" + appname);
        Util.writeString(appname, out);
        
        String unit = ConfigKeeper.configMap.get(appLog.getAppName())
                .getString(AppConfigurationConstants.TRANSFER_PERIOD_UNIT, "hour");
        int value = ConfigKeeper.configMap.get(appLog.getAppName())
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

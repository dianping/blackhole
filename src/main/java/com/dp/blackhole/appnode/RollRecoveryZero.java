package com.dp.blackhole.appnode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.AppConfigurationConstants;
import com.dp.blackhole.conf.ConfigKeeper;

public class RollRecoveryZero implements Runnable{
    private static final Log LOG = LogFactory.getLog(RollRecoveryZero.class);
    private String collectorServer;
    private AppLog appLog;
    private long rollTimestamp;
    private Socket server;
    public RollRecoveryZero(String collectorServer, AppLog appLog, long rollTimestamp) {
        this.collectorServer = collectorServer;
        this.appLog = appLog;
        this.rollTimestamp = rollTimestamp;
    }
    
    @Override
    public void run() {
        String unit = ConfigKeeper.configMap.get(appLog.getAppName())
                .getString(AppConfigurationConstants.TRANSFER_PERIOD_UNIT, "hour");
        SimpleDateFormat unitFormat = new SimpleDateFormat(Util.getFormatByUnit(unit));
        String rollIdent = unitFormat.format(rollTimestamp);
        File rolledFile = Util.findRealFileByIdent(appLog.getTailFile(), rollIdent);
        if (rolledFile == null) {
            LOG.error("Can not find the file match rollTimestamp " + rollTimestamp);
            return;
        }
        SocketChannel socketChannel = null;
        DataOutputStream out = null;
        DataInputStream in = null;
        long offset = 0;
        int port = 0;
        try {
            //Unblocking IO zero copy
            port = ConfigKeeper.configMap.get(appLog.getAppName())
                    .getInteger(AppConfigurationConstants.PORT);
            SocketAddress address = new InetSocketAddress(collectorServer, port);
            socketChannel = SocketChannel.open();
            socketChannel.connect(address);
            socketChannel.configureBlocking(true);//TODO to be review
            
            //Blocking IO
            server = socketChannel.socket();
            out = sendHeaderReq();
            offset = receiveHeaderRes(in);
            LOG.info("Seek to the position " + offset + " ok. Begin to transfer...");

            FileChannel fc = new FileInputStream(rolledFile).getChannel();
            long curnset =    fc.transferTo(offset, rolledFile.length(), socketChannel);
            System.out.println(curnset);
            LOG.info("Roll file " + rolledFile + " has been transfered, ");
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
                if (socketChannel != null) {
                    socketChannel.close();
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

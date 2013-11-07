package com.dp.blackhole.appnode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.conf.ConfigKeeper;

public class RollRecoveryZero implements Runnable{
    private static final Log LOG = LogFactory.getLog(RollRecoveryZero.class);
    private String collectorServer;
    private int port;
    private AppLog appLog;
    private long rollTimestamp;
    private Socket server;
    public RollRecoveryZero(String collectorServer, int port, AppLog appLog, long rollTimestamp) {
        this.collectorServer = collectorServer;
        this.port = port;
        this.appLog = appLog;
        this.rollTimestamp = rollTimestamp;
    }
    
    @Override
    public void run() {
        long period = ConfigKeeper.configMap.get(appLog.getAppName())
                .getLong(ParamsKey.Appconf.ROLL_PERIOD, 3600l);
        SimpleDateFormat unitFormat = new SimpleDateFormat(Util.getFormatFromPeroid(period));
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
        FileInputStream fileStream = null;
        try {
            //blocking IO zero copy
            SocketAddress address = new InetSocketAddress(collectorServer, port);
            socketChannel = SocketChannel.open();
            socketChannel.connect(address);
            socketChannel.configureBlocking(true);
            
            //Blocking IO
            server = socketChannel.socket();
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
            LOG.info("Seek to the position " + offset + " ok. Begin to transfer...");

            fileStream = new FileInputStream(rolledFile);
            fileStream.getChannel().transferTo(offset, rolledFile.length(), socketChannel);
            LOG.info("Roll file " + rolledFile + " has been transfered, ");
        } catch (IOException e) {
            LOG.error("Oops, got an exception:", e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
                if (fileStream != null) {
                    fileStream.close();
                }
                if (socketChannel != null) {
                    socketChannel.close();
                }
            } catch (IOException e) {
                LOG.warn("Oops, got an exception:", e);
            }
        }
    }
}

package com.dp.blackhole.appnode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
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
    private final long rollTimestamp;
    private Socket socket;
    private byte[] inbuf;
    private Appnode node;
    public RollRecovery(Appnode node, String collectorServer, int port, AppLog appLog, long rollTimestamp) {
        this.node = node;
        this.collectorServer = collectorServer;
        this.port = port;
        this.appLog = appLog;
        this.rollTimestamp = rollTimestamp;
        this.inbuf = new byte[DEFAULT_BUFSIZE];
    }

    public void stop() {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException e) {
                LOG.warn("Warnning, clean fail:", e);
            }
            socket = null;
        }
    }

    @Override
    public void run() {
        LOG.info("Roll Recovery for " + appLog + " running...");
        long period = ConfigKeeper.configMap.get(appLog.getAppName())
                .getLong(ParamsKey.Appconf.ROLL_PERIOD, 3600l);
        SimpleDateFormat unitFormat = new SimpleDateFormat(Util.getFormatFromPeroid(period));
        String rollIdent = unitFormat.format(rollTimestamp);
        File rolledFile = Util.findRealFileByIdent(appLog.getTailFile(), rollIdent);
        if (rolledFile == null || !rolledFile.exists()) {
            LOG.error("app " + appLog.getAppName() + " rollIdent " + rollIdent + " can not be found");
            node.reportUnrecoverable(appLog.getAppName(), node.getHost(), rollTimestamp);
            return;
        }
        LOG.debug("roll file is " + rolledFile);
        RandomAccessFile reader = null;
        DataOutputStream out = null;
        DataInputStream in = null;
        long offset = 0;
        AgentProtocol protocol = null;
        try {
            socket = new Socket(collectorServer, port);
            reader = new RandomAccessFile(rolledFile, RAF_MODE);
            out = new DataOutputStream(socket.getOutputStream());
            in = new DataInputStream(socket.getInputStream());
        } catch (IOException e) {
            LOG.error("Faild to build recovery stream.", e);
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
                reader = null;
            }
            stop();
            node.reportUnrecoverable(appLog.getAppName(), node.getHost(), rollTimestamp);
            return;
        }

        try {
            protocol = new AgentProtocol();
            AgentHead head = protocol.new AgentHead();
            head.type = AgentProtocol.RECOVERY;
            head.app = appLog.getAppName();
            head.peroid = period;
            head.ts = rollTimestamp;
            protocol.sendHead(out, head);
        } catch (IOException e) {
            LOG.error("Protocol head send fail, report recovery fail from appnode. ", e);
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
                reader = null;
            }
            stop();
            node.reportRecoveryFail(appLog.getAppName(), node.getHost(), rollTimestamp);
            return;
        }

        try {
            offset = protocol.receiveOffset(in);
            LOG.info("Received offset [" + offset + "] in header response.");
            reader.seek(offset);
            LOG.info("Seeked to the position " + offset + ". Begin to transfer...");
            int len = 0;
            long transferBytes = 0;
            while ((len = reader.read(inbuf)) != -1) {
                out.write(inbuf, 0, len);
                transferBytes += len;
            }
            out.flush();
            LOG.info("Roll file transfered, including [" + transferBytes + "] bytes.");
        } catch (IOException e) {
            LOG.error("Recover stream broken.", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                }
                reader = null;
            }
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
                in = null;
            }
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                }
                out = null;
            }
            node.removeRecoverying(appLog.getAppName(), rollTimestamp);
        }
    }
}
package com.dp.blackhole.appnode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.cat.Cat;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.exception.BlackholeClientException;

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
    public RollRecovery(Appnode node, String collectorServer, int port, AppLog appLog, final long rollTimestamp) {
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

    private void stopRecoverying() {
        stop();
        node.removeRecoverying(appLog.getAppName(), rollTimestamp);
    }

    private void stopRecoveryingCauseException(String desc, Exception e) {
        LOG.error(desc, e);
        Cat.logError(desc, e);
        stopRecoverying();
    }

    @Override
    public void run() {
        LOG.info("Roll Recovery for " + appLog + " running...");
        // check socket connection
        DataOutputStream out = null;
        DataInputStream in = null;
        try {
            socket = new Socket(collectorServer, port);
            out = new DataOutputStream(socket.getOutputStream());
            in = new DataInputStream(socket.getInputStream());
        } catch (IOException e) {
            stopRecoveryingCauseException("Faild to build recovery stream.", e);
            node.reportRecoveryFail(appLog.getAppName(), node.getHost(), rollTimestamp);
            return;
        }

        // check local file existence 
        long period = ConfigKeeper.configMap.get(appLog.getAppName())
                .getLong(ParamsKey.Appconf.ROLL_PERIOD, 3600l);
        SimpleDateFormat unitFormat = new SimpleDateFormat(Util.getFormatFromPeroid(period));
        String rollIdent = unitFormat.format(rollTimestamp);
        File rolledFile = Util.findRealFileByIdent(appLog.getTailFile(), rollIdent);
        File gzFile = Util.findGZFileByIdent(appLog.getTailFile(), rollIdent);
        if (!rolledFile.exists() && (gzFile == null || !gzFile.exists())) {
            LOG.error("Can not found both " + rolledFile + " add " + gzFile);
            Cat.logError(new BlackholeClientException("Can not found both " + rolledFile + " add " + gzFile));
            stopRecoverying();
            node.reportUnrecoverable(appLog.getAppName(), node.getHost(), rollTimestamp);
            return;
        }

        // send recovery head, report fail in appnode if catch exception.
        AgentProtocol protocol = null;
        try {
            protocol = wrapSendRecoveryHead(out, period);
        } catch (IOException e) {
            stopRecoveryingCauseException("Protocol head send fail, report recovery fail from appnode.", e);
            node.reportRecoveryFail(appLog.getAppName(), node.getHost(), rollTimestamp);
            return;
        }

        // receive begin offset, do not report anything if catch exception.
        long offset = 0;
        try {
            offset = protocol.receiveOffset(in);
            LOG.info("Received offset [" + offset + "] in header response.");
        } catch (IOException e) {
            stopRecoveryingCauseException("Recover stream broken. just report recovery fail from collectornode.", e);
            return;
        }

        int len = 0;
        long transferBytes = 0;
        if (rolledFile.exists()) {
            // recovery if raw rolledfile exists, using RandomAccessFile
            LOG.debug("roll file is " + rolledFile);
            RandomAccessFile reader = null;
            try {
                reader = new RandomAccessFile(rolledFile, RAF_MODE);
            } catch (FileNotFoundException e) {
                stopRecoveryingCauseException("Oops! It not should be happen here.", e);
                return;
            }
            try {
                reader.seek(offset);
                LOG.info("Seeked to the position " + offset + ". Begin to transfer...");
                while ((len = reader.read(inbuf)) != -1) {
                    out.write(inbuf, 0, len);
                    transferBytes += len;
                }
                out.flush();
                LOG.info("Roll file transfered, including [" + transferBytes + "] bytes.");
            } catch (IOException e) {
                LOG.error("Recover stream broken.", e);
                Cat.logError("Recover stream broken.", e);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                    }
                    reader = null;
                }
                stopRecoverying();
            }
        } else if (gzFile != null && gzFile.exists()) {
            // recovery if and only if gz file exists, using GZInputstream
            LOG.info("Can not found " + rolledFile + ", trying gz file.");
            GZIPInputStream gin = null;
            try {
                gin = new GZIPInputStream(new FileInputStream(gzFile));
            } catch (IOException e) {
                stopRecoveryingCauseException("Create GZIP stream fail.", e);
                return;
            }
            try {
                gin.skip(offset);
                while((len = gin.read(inbuf)) != -1) {
                    out.write(inbuf, 0, len);
                    transferBytes += len;
                }
                out.flush();
                LOG.info("Roll file transfered, including [" + transferBytes + "] bytes.");
            } catch (IOException e) {
                LOG.error("Recover stream broken.", e);
                Cat.logError("Recover stream broken.", e);
            } finally {
                if (gin != null) {
                    try {
                        gin.close();
                    } catch (IOException e) {
                    }
                    gin = null;
                }
                stopRecoverying();
            }
        } else {
            LOG.error("Oops! It not should be happen here.");
            Cat.logError(new BlackholeClientException("Oops! It not should be happen here."));
            stopRecoverying();
            return;
        }
    }

    private AgentProtocol wrapSendRecoveryHead(DataOutputStream out, long period)
            throws IOException {
        AgentProtocol protocol = new AgentProtocol();
        AgentHead head = protocol.new AgentHead();
        head.type = AgentProtocol.RECOVERY;
        head.app = appLog.getAppName();
        head.peroid = period;
        head.ts = rollTimestamp;
        protocol.sendHead(out, head);
        return protocol;
    }
}
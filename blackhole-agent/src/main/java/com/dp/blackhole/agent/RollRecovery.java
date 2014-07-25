package com.dp.blackhole.agent;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.cat.Cat;
import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.exception.BlackholeClientException;

public class RollRecovery implements Runnable{
    private static final Log LOG = LogFactory.getLog(RollRecovery.class);
    private static final int DEFAULT_BUFSIZE = 8192;
    private String brokerServer;
    private int port;
    private TopicMeta topicMeta;
    private final long rollTimestamp;
    private Socket socket;
    private byte[] inbuf;
    private Agent node;
    private boolean isFinal;
    public RollRecovery(Agent node, String brokerServer, int port, TopicMeta topicMeta, final long rollTimestamp, boolean isFinal) {
        this.node = node;
        this.brokerServer = brokerServer;
        this.port = port;
        this.topicMeta = topicMeta;
        this.rollTimestamp = rollTimestamp;
        this.inbuf = new byte[DEFAULT_BUFSIZE];
        this.isFinal = isFinal;
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
        node.removeRecoverying(topicMeta.getMetaKey(), rollTimestamp);
    }

    private void stopRecoveryingCauseException(String desc, Exception e) {
        LOG.error(desc, e);
        Cat.logError(desc, e);
        stopRecoverying();
    }

    @Override
    public void run() {
        // check local file existence 
        long period = topicMeta.getRollPeriod();
        long fileSize = 0;
        File transferFile = null;
        boolean hasCompressed = false;
        SimpleDateFormat unitFormat = new SimpleDateFormat(Util.getFormatFromPeroid(period));
        String rollIdent = unitFormat.format(rollTimestamp);
        LOG.info("Recoverying " + rollIdent + " " + topicMeta);
        File rolledFile = Util.findRealFileByIdent(topicMeta.getTailFile(), rollIdent);
        File gzFile = Util.findGZFileByIdent(topicMeta.getTailFile(), rollIdent);
        if (!rolledFile.exists() && (gzFile == null || !gzFile.exists())) {
            LOG.error("Can not found both " + rolledFile + " and gzFile");
            Cat.logError(new BlackholeClientException("Can not found both " + rolledFile + " and gzFile"));
            stopRecoverying();
            node.reportUnrecoverable(topicMeta.getMetaKey(), node.getHost(), period, rollTimestamp);
            return;
        }

        // send recovery head, report fail in agent if catch exception.
        DataOutputStream out = null;
        try {
            socket = new Socket(brokerServer, port);
            out = new DataOutputStream(socket.getOutputStream());
            if (rolledFile.exists()) {
                transferFile = rolledFile;
                fileSize = rolledFile.length();
            } else if (gzFile != null && gzFile.exists()) {
                fileSize = gzFile.length();
                transferFile = gzFile;
                hasCompressed = true;
            } else {
                throw new IOException("Can not found both " + rolledFile + " and gzFile");
            }
            wrapSendRecoveryHead(out, fileSize, hasCompressed, isFinal);
        } catch (IOException e) {
            stopRecoveryingCauseException("Faild to build recovery stream or send protocol header.", e);
            node.reportRecoveryFail(topicMeta.getMetaKey(), node.getHost(), period, rollTimestamp, isFinal);
            return;
        }

        int len = 0;
        long transferBytes = 0;
        BufferedInputStream is;
        try {
            is = new BufferedInputStream(new FileInputStream(transferFile));
        } catch (FileNotFoundException e) {
            stopRecoveryingCauseException(transferFile + " missing. It not should be happen here.", e);
            return;
        }
        try {
            LOG.info(transferFile + " is transferring...");
            while ((len = is.read(inbuf)) != -1) {
                out.write(inbuf, 0, len);
                transferBytes += len;
            }
            out.flush();
            LOG.info(transferFile + " transfered, including [" + transferBytes + "] bytes.");
        } catch (IOException e) {
            LOG.error("Recover stream broken.", e);
            Cat.logError("Recover stream broken.", e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    LOG.error("Can not close input stream.", e);
                }
                is = null;
            }
            stopRecoverying();
        }
    }

    private AgentProtocol wrapSendRecoveryHead(DataOutputStream out, long fileSize, boolean hasCompressed, boolean isFinal)
            throws IOException {
        AgentProtocol protocol = new AgentProtocol();
        AgentHead head = protocol.new AgentHead();
        head.instanceId = topicMeta.getInstanceId();
        if (head.instanceId == null) {
            head.type = AgentProtocol.RECOVERY;
        } else {
            head.type = AgentProtocol.RECOVERY_IN_PAAS;
        }
        head.app = topicMeta.getTopic();
        head.peroid = topicMeta.getRollPeriod();
        head.ts = rollTimestamp;
        head.size = fileSize;
        head.hasCompressed = hasCompressed;
        head.isFinal = isFinal;
        protocol.sendHead(out, head);
        return protocol;
    }
}
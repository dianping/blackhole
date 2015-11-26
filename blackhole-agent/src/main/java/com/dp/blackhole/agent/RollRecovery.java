package com.dp.blackhole.agent;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.agent.persist.IRecoder;
import com.dp.blackhole.agent.persist.Record;
import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.network.TransferThrottler;

public class RollRecovery implements Runnable{
    private static final Log LOG = LogFactory.getLog(RollRecovery.class);
    private static final int DEFAULT_BUFSIZE = 8192;
    private String brokerServer;
    private int port;
    private AgentMeta topicMeta;
    private final long rollTimestamp;
    private Socket socket;
    private byte[] inbuf;
    private Agent node;
    private boolean isFinal;
    private boolean isPersist;
    private final IRecoder state;
    private TransferThrottler throttler;
    private File transferFile;
    private boolean isTransferFileCompressed;

    public RollRecovery(Agent node, String brokerServer, int port,
            AgentMeta topicMeta, final long rollTimestamp, boolean isFinal,
            boolean isPersist, IRecoder state) {
        this.node = node;
        this.brokerServer = brokerServer;
        this.port = port;
        this.topicMeta = topicMeta;
        this.rollTimestamp = rollTimestamp;
        this.inbuf = new byte[DEFAULT_BUFSIZE];
        this.isFinal = isFinal;
        this.isPersist = isPersist;
        this.state = state;
        if (topicMeta.getBandwidthPerSec() > 0) {
            this.throttler = new TransferThrottler(topicMeta.getBandwidthPerSec());
        }
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
        node.removeRecoverying(topicMeta.getTopicId(), rollTimestamp);
    }

    @Override
    public void run() {
        long rollPeriod = topicMeta.getRollPeriod();
        long toTransferSize = 0;
        DataOutputStream out = null;
        String rollString = Util.formatTs(rollTimestamp, rollPeriod);
        InputStream is = null;
        LOG.info("Begin to recoverying: broker=" + brokerServer
                + " rollTS=" + rollTimestamp + " isFinal=" + " isPersist=" + isPersist
                + " meta=" + topicMeta);
        try {
            //sleep random seconds to avoid the rarely situation which latest rotation record not found
            Thread.sleep((new Random().nextInt(10) + 10) * 1000);
        } catch (InterruptedException e) {
            LOG.warn("Thread interrupted. ", e);
            node.reportRecoveryFail(topicMeta.getTopicId(), topicMeta.getSource(), rollPeriod, rollTimestamp, isFinal);
            return;
        }
        try {
            //retrive record to got recovery offset
            Record record = state.retrive(rollTimestamp);
            //no need to recovery:
            //1. loss record, the missing data will recovery into the last missing stage
            //2. no persist topic
            if (record == null || !isPersist) {
                sendIgnoranceToBroker(rollPeriod, toTransferSize, isTransferFileCompressed, rollString);
                return;
            }
            
            try {
                findAppropriateTransferFile(record);
            } catch (FileNotFoundException e) {
                LOG.error(e.getMessage());
                node.reportUnrecoverable(topicMeta.getTopicId(), topicMeta.getSource(), rollPeriod, rollTimestamp, isFinal, isPersist);
                return;
            }
            
            // open stream
            long from = record.getStartOffset();
            long to = record.getEndOffset();
            try {
                if (isTransferFileCompressed && to == LogReader.END_OFFSET_OF_FILE) {
                    //If file compressed and end offset was EOF,
                    //end offset set to length of compressed file and transfer it.
                    is = new BufferedInputStream(new FileInputStream(transferFile), 65536);
                    from = LogReader.BEGIN_OFFSET_OF_FILE;
                    to = transferFile.length() - 1; //minus one to convert offset (from 0)
                } else if (isTransferFileCompressed) {
                    //If file compressed but end offset was specified,
                    //transfer the decompressed file with this specified end offset.
                    is = new GZIPInputStream(new FileInputStream(transferFile), 65536);
                } else {
                    //If file uncompressed,
                    //transfer the uncompressed file with its specified end offset.
                    is = new BufferedInputStream(new FileInputStream(transferFile), 65536);
                }
                
            } catch (IOException e) {
                LOG.error("Can not open an input stream for " + transferFile, e);
                node.reportUnrecoverable(topicMeta.getTopicId(), topicMeta.getSource(), rollPeriod, rollTimestamp, isFinal, isPersist);
                return;
            }
            
            // correct end offset:
            // RESUME occur at begin of rotate period, its RollTs will be set to the last RotateTs
            // so that the start offset may be bigger than end offset. It should correct to the end of file.
            if (to < from) {
                to = transferFile.length() - 1;
                LOG.info("Rorrecting " + topicMeta.getTopicId() + "'s record: " + record +  ": from > to, recovery to " + to);
            }
    
            // send recovery head, report fail in agent if catch exception.
            try {
                socket = new Socket(brokerServer, port);
                out = new DataOutputStream(socket.getOutputStream());
                toTransferSize = to - from + 1;
                LOG.info("Perpare to Recovery " + transferFile + " for " + rollString 
                        + " offset [" + from + "~" + to + "] " 
                        + " include " + toTransferSize);
                wrapSendRecoveryHead(false, out, toTransferSize, isTransferFileCompressed, isFinal);
            } catch (IOException e) {
                LOG.error("Faild to build recovery stream or send protocol header.", e);
                node.reportRecoveryFail(topicMeta.getTopicId(), topicMeta.getSource(), rollPeriod, rollTimestamp, isFinal);
                return;
            }
    
            transferData(rollPeriod, toTransferSize, out, rollString, is, from);
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

    private void transferData(long rollPeriod, long toTransferSize,
            DataOutputStream out, String rollString, InputStream is, long from) {
        int len = 0;
        long transferBytes = 0;
        try {
            LOG.info(transferFile + " is transferring for " + rollString);
            is.skip(from);
            while (toTransferSize > 0 && (len = is.read(inbuf)) != -1) {
                if (len > toTransferSize) {
                    len = (int) toTransferSize;
                }
                out.write(inbuf, 0, len);
                transferBytes += len;
                toTransferSize -= len;
                if (throttler != null) {
                    throttler.throttle(len);
                }
            }
            out.flush();
            LOG.info(transferFile + " transfered for " + rollString + ", including [" + transferBytes + "] bytes.");
        } catch (IOException e) {
            LOG.error("Recover stream broken.", e);
            node.reportRecoveryFail(topicMeta.getTopicId(), topicMeta.getSource(), rollPeriod, rollTimestamp, isFinal);
        }
    }

    private void findAppropriateTransferFile(Record record) throws FileNotFoundException {
        File rolledFile;
        File gzFile = null;
        long rotatePeriod= topicMeta.getRotatePeriod();
        //use origin tail file to recovery because that the rotation is belong to the current rotate stage
        long rotation = record.getRotation();
        if (isFinal || Util.belongToSameRotate(Util.getTS(), rotation, topicMeta.getRotatePeriod())) {
            rolledFile = new File(topicMeta.getTailFile());
        } else {
            String rotateString = Util.formatTs(rotation, rotatePeriod);
            rolledFile = Util.findRealFileByIdent(topicMeta.getTailFile(), rotateString);
            gzFile = Util.findGZFileByIdent(topicMeta.getTailFile(), rotateString);
        }
        
        if (rolledFile.exists()) {
            transferFile = rolledFile;
        } else if (gzFile != null && gzFile.exists()) {
            transferFile = gzFile;
            isTransferFileCompressed = true;
        } else if (!Util.belongToSameRotate(record.getRollTs(), rotation, topicMeta.getRotatePeriod())) {
            //use current watching file again because that this is a special case which file rotate hasn't happened yet
            transferFile = new File(topicMeta.getTailFile());
            LOG.warn("using SPECIAL TRANSFER FILE(" + transferFile + ") because file rotate hasn't happened yet");
        } else {
            throw new FileNotFoundException("Can not found both " + rolledFile + " and gzFile");
        }
    }

    private void sendIgnoranceToBroker(final long rollPeriod,
            final long toTransferSize, final boolean hasCompressed,
            final String rollString) {
        try {
            socket = new Socket(brokerServer, port);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            if (isPersist) {
                LOG.info("Can not found " + topicMeta.getTopicId() + "'s Record for " + rollString + "[" + rollTimestamp + "], send ignorance to broker.");
            } else {
                LOG.info("No need to recovery the topic which no need persist: " + topicMeta.getTopicId());
            }
            wrapSendRecoveryHead(true, out, toTransferSize, hasCompressed, isFinal);
        } catch (IOException e) {
            LOG.error("Faild to send ignorance protocol header.", e);
            node.reportUnrecoverable(topicMeta.getTopicId(), topicMeta.getSource(), rollPeriod, rollTimestamp, isFinal, isPersist);
        }
    }

    public AgentProtocol wrapSendRecoveryHead(boolean ignore, DataOutputStream out, long fileSize, boolean hasCompressed, boolean isFinal)
            throws IOException {
        AgentProtocol protocol = new AgentProtocol();
        AgentHead head = protocol.new AgentHead();
        head.version = AgentProtocol.VERSION_MICOR_BATCH;
        head.ignore = ignore;
        head.app = topicMeta.getTopic();
        head.source = topicMeta.getSource();
        head.period = topicMeta.getRollPeriod();
        head.ts = rollTimestamp;
        head.size = fileSize;
        head.hasCompressed = hasCompressed;
        head.isFinal = isFinal;
        protocol.sendHead(out, head);
        return protocol;
    }
}
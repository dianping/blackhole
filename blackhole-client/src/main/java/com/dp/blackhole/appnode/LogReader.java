package com.dp.blackhole.appnode;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.cat.Cat;
import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.conf.ConfigKeeper;

public class LogReader implements Runnable{
    private static final Log LOG = LogFactory.getLog(LogReader.class);
    private static final int IN_BUF = 1024 * 8;
    private static final int SYS_MAX_LINE_SIZE = 1024 * 512;

    private AppLog appLog;
    private Appnode node;
    private String collectorServer;
    private int port;
    private Socket socket;
    EventWriter eventWriter;
    
    public LogReader(Appnode node, String collectorServer, int port, AppLog appLog) {
        this.node = node;
        this.collectorServer = collectorServer;
        this.port = port;
        this.appLog = appLog;
    }

    public void stop() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
                socket = null;
            }
        } catch (IOException e) {
            LOG.warn("Failed to close socket.", e);
            Cat.logError("Failed to close socket.", e);
        }
        node.getListener().unregisterLogReader(appLog.getTailFile());
    }

    @Override
    public void run() {
        try {
            LOG.info("Log reader for " + appLog + " running...");
            socket = new Socket(collectorServer, port);
            AgentProtocol protocol = new AgentProtocol();
            AgentHead head = protocol.new AgentHead();
            head.type = AgentProtocol.STREAM;
            head.app = appLog.getAppName();
            head.peroid = ConfigKeeper.configMap.get(appLog.getAppName()).getLong(ParamsKey.Appconf.ROLL_PERIOD);
            protocol.sendHead(new DataOutputStream(socket.getOutputStream()), head);
            
            File tailFile = new File(appLog.getTailFile());
            this.eventWriter = new EventWriter(tailFile, appLog.getMaxLineSize());
            
            if (!node.getListener().registerLogReader(appLog.getTailFile(), this)) {
                throw new IOException("Failed to register a log reader for " + appLog.getAppName() 
                        + " with " + appLog.getTailFile() + ", thread will not run.");
            }
        } catch (UnknownHostException e) {
            LOG.error("Socket fail!", e);
            Cat.logError("Socket fail!", e);
            node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
        } catch (IOException e) {
            LOG.error("Oops, got an exception", e);
            Cat.logError("Oops, got an exception", e);
            node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
        } catch (RuntimeException e) {
            LOG.error("Oops, got an RuntimException:" , e);
            Cat.logError("Oops, got an RuntimException:" , e);
            node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
        }
    }

    public String getAppName() {
        return this.appLog.getAppName();
    }

    class EventWriter {
        private final File file;
        private final byte inbuf[] = new byte[IN_BUF];
        private final OutputStream out = socket.getOutputStream();
        private RandomAccessFile reader;
        private final int maxLineSize;
        private final ByteArrayOutputStream lineBuf;
        private boolean accept;
        
        public EventWriter(final File file, int maxLineSize) throws IOException {
            this.file = file;
            if (maxLineSize > SYS_MAX_LINE_SIZE) {
                this.maxLineSize = SYS_MAX_LINE_SIZE;
            } else {
                this.maxLineSize = maxLineSize;
            }
            this.reader = new RandomAccessFile(file, "r");
            this.lineBuf = new ByteArrayOutputStream(maxLineSize);
            this.accept = true;
        }
        
        public void processRotate() {
            try {
                final RandomAccessFile save = reader;
                reader = new RandomAccessFile(file, "r");
                // At this point, we're sure that the old file is rotated
                // Finish scanning the old file and then we'll start with the new one
                readLines(save);
                closeQuietly(save);
                out.write('\n'); //make server easy to handle
                out.flush();
            } catch (IOException e) {
                LOG.error("Oops, got an exception:", e);
                Cat.logError("Oops, got an exception:", e);
                closeQuietly(reader);
                closeQuietly(out);
                LOG.debug("process rotate failed, stop.");
                stop();
                node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
            }
        }
        
        public void process() {
            try {
                readLines(reader);
            } catch (IOException e) {
                LOG.error("Oops, process read lines fail:", e);
                Cat.logError("Oops, process read lines fail:", e);
                closeQuietly(reader);
                closeQuietly(out);
                LOG.debug("process failed, stop.");
                stop();
                node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
            }
        }
        
        private long readLines(RandomAccessFile reader) throws IOException {
            long pos = reader.getFilePointer();
            long rePos = pos; // position to re-read
            int num;
            while ((num = reader.read(inbuf)) != -1) {
                for (int i = 0; i < num; i++) {
                    final byte ch = inbuf[i];
                    switch (ch) {
                    case '\n':
                        if (accept) {
                            handleLine(lineBuf.toByteArray());
                        }
                        accept = true;
                        lineBuf.reset();
                        rePos = pos + i + 1;
                        break;
                    case '\r':
                        break;
                    default:
                        if (accept) {
                            lineBuf.write(ch);
                        }
                        if (accept && lineBuf.size() == maxLineSize) {
                            LOG.warn("length of this line is longer than maxLineSize " + maxLineSize + ", discard.");
                            accept = false;
                        }
                    }
                }
                pos = reader.getFilePointer();
            }
            lineBuf.reset(); // not strictly necessary
            reader.seek(rePos); // Ensure we can re-read if necessary
            return rePos;
        }
        
        private void handleLine(byte[] line) throws IOException {
            out.write(line);
            out.write('\n'); //make server easy to handle
            Cat.logEvent("BHLineStat", LogReader.this.appLog.getAppName());
        }
        
        /**
         * Unconditionally close a Closeable.
         * Equivalent to close(), except any exceptions will be ignored.
         * This is typically used in finally blocks.
         */
        private void closeQuietly(Closeable closeable) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            } catch (IOException ioe) {
                // ignore
            }
        }
    }
}
package com.dp.blackhole.appnode;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.conf.ConfigKeeper;

public class LogReader implements Runnable{
    private static final Log LOG = LogFactory.getLog(LogReader.class);
    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    private AppLog appLog;
    private Appnode node;
    private String collectorServer;
    private int port;
    private int bufSize;
    private Socket socket;
    EventWriter eventWriter;
    
    public LogReader(Appnode node, String collectorServer, int port, AppLog appLog) {
        this.node = node;
        this.collectorServer = collectorServer;
        this.port = port;
        this.appLog = appLog;
        this.bufSize = ConfigKeeper.configMap.get(appLog.getAppName()).getInteger(ParamsKey.Appconf.BUFFER_SIZE, 4096);
    }

    public void stop() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
                socket = null;
            }
        } catch (IOException e) {
            LOG.warn("Warnning, clean fail:", e);
        }
        LOG.debug("Perpare to unregister LogReader: " + this.toString() + ", with " + appLog.getTailFile());
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
            this.eventWriter = new EventWriter(tailFile, bufSize);
            
            if (!node.getListener().registerLogReader(appLog.getTailFile(), this)) {
                throw new IOException("Failed to register a log reader for " + appLog.getAppName() 
                        + " with " + appLog.getTailFile() + ", thread will not run.");
            }
        } catch (UnknownHostException e) {
            LOG.error("Socket fail!", e);
            node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
        } catch (IOException e) {
            LOG.error("Oops, got an exception", e);
            node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
        } catch (RuntimeException e) {
            LOG.error("Oops, got an RuntimException:" , e);
            node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
        }
    }

    class EventWriter {
        private final Charset cset;
        private final File file;
        private final byte inbuf[];
        private OutputStreamWriter writer;
        private RandomAccessFile reader;
        
        public EventWriter(final File file, final int bufSize) throws IOException {
            this(file, bufSize, DEFAULT_CHARSET);
        }
        public EventWriter(final File file, final int bufSize, Charset cset) throws IOException {
            this.file = file;
            this.inbuf = new byte[bufSize];
            this.cset = cset;
            writer = new OutputStreamWriter(socket.getOutputStream());
            this.reader = new RandomAccessFile(file, "r");
        }
        
        public void processRotate() {
            try {
                final RandomAccessFile save = reader;
                reader = new RandomAccessFile(file, "r");
                // At this point, we're sure that the old file is rotated
                // Finish scanning the old file and then we'll start with the new one
                readLines(save);
                closeQuietly(save);
                writer.write('\n'); //make server easy to handle
                writer.flush();
            } catch (IOException e) {
                LOG.error("Oops, got an exception:", e);
                closeQuietly(reader);
                closeQuietly(writer);
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
                closeQuietly(reader);
                closeQuietly(writer);
                LOG.debug("process failed, stop.");
                stop();
                node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
            }
        }
        
        private long readLines(RandomAccessFile reader) throws IOException {
            ByteArrayOutputStream lineBuf = new ByteArrayOutputStream(64);
            long pos = reader.getFilePointer();
            long rePos = pos; // position to re-read
            int num;
            while ((num = reader.read(inbuf)) != -1) {
                for (int i = 0; i < num; i++) {
                    final byte ch = inbuf[i];
                    switch (ch) {
                    case '\n':
                        handleLine(new String(lineBuf.toByteArray(), cset));
                        lineBuf.reset();
                        rePos = pos + i + 1;
                        break;
                    default:
                        lineBuf.write(ch);
                    }
                }
                pos = reader.getFilePointer();
            }
            closeQuietly(lineBuf); // not strictly necessary
            reader.seek(rePos); // Ensure we can re-read if necessary
            return rePos;
        }
        
        private void handleLine(String line) throws IOException {
            writer.write(line);
            writer.write('\n'); //make server easy to handle
            writer.flush();
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
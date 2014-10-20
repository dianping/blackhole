package com.dp.blackhole.agent;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.protocol.data.LastRotateRequest;
import com.dp.blackhole.protocol.data.ProduceRequest;
import com.dp.blackhole.protocol.data.RegisterRequest;
import com.dp.blackhole.protocol.data.RotateRequest;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;

public class LogReader implements Runnable{
    private static final Log LOG = LogFactory.getLog(LogReader.class);
    private static final int IN_BUF = 1024 * 8;
    private static final int SYS_MAX_LINE_SIZE = 1024 * 512;

    private TopicMeta topicMeta;
    private Agent agent;
    private String source;
    private String broker;
    private int brokerPort;
    private Socket socket;
    EventWriter eventWriter;
    
    public LogReader(Agent agent, String localhost, String broker, int port, TopicMeta topicMeta) {
        this.agent = agent;
        this.source =  Util.getSource(localhost, topicMeta.getInstanceId());
        this.broker = broker;
        this.brokerPort = port;
        this.topicMeta = topicMeta;
    }

    public void stop() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
                socket = null;
            }
        } catch (IOException e) {
            LOG.warn("Failed to close socket.", e);
        }
        agent.getListener().unregisterLogReader(topicMeta.getTailFile());
    }

    @Override
    public void run() {
        try {
            LOG.info("Log reader for " + topicMeta + " running...");
            
            File tailFile = new File(topicMeta.getTailFile());
            this.eventWriter = new EventWriter(tailFile, topicMeta.getMaxLineSize());
            
            if (!agent.getListener().registerLogReader(topicMeta.getTailFile(), this)) {
                throw new IOException("Failed to register a log reader for " + topicMeta.getMetaKey() 
                        + " with " + topicMeta.getTailFile() + ", thread will not run.");
            }
        } catch (Throwable t) {
            LOG.error("Oops, got an exception", t);
            agent.reportFailure(topicMeta.getMetaKey(), source, Util.getTS());
        }
    }

    class EventWriter {
        private final File file;

        private final byte inbuf[] = new byte[IN_BUF];
        private SocketChannel channel;
        private RandomAccessFile reader;
        private final int maxLineSize;
        private final ByteArrayOutputStream lineBuf;
        private boolean accept;
        private ByteBuffer messageBuffer;
        private int messageNum;
        
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
            
            messageBuffer = ByteBuffer.allocate(512 * 1024);
            
            
            channel = SocketChannel.open();
            channel.connect(new InetSocketAddress(broker, brokerPort));
            socket = channel.socket();
            LOG.info("connected broker: " + broker + ":" + brokerPort);
            doStreamReg();
        }
        
        private void doStreamReg() throws IOException {
            RegisterRequest request = new RegisterRequest(
                    topicMeta.getTopic(),
                    source,
                    topicMeta.getRollPeriod(),
                    broker);
            TransferWrap wrap = new TransferWrap(request);
            wrap.write(channel);
        }
        
        public void processRotate() {
            try {
                final RandomAccessFile save = reader;
                reader = new RandomAccessFile(file, "r");
                // At this point, we're sure that the old file is rotated
                // Finish scanning the old file and then we'll start with the new one
                readLines(save);
                closeQuietly(save);
                sendMessage();
                //do not handle log rotation any more when dying
                if (topicMeta.isDying()) {
                    return;
                }
                RotateRequest request = new RotateRequest(
                        topicMeta.getTopic(),
                        source,
                        topicMeta.getRollPeriod());
                TransferWrap wrap = new TransferWrap(request);
                wrap.write(channel);
            } catch (IOException e) {
                LOG.error("Oops, got an exception:", e);
                closeQuietly(reader);
                closeChannelQuietly(channel);
                LOG.debug("process rotate failed, stop.");
                stop();
                agent.reportFailure(topicMeta.getMetaKey(), source, Util.getTS());
            }
        }
        
        public void processLastRotate() {
            try {
                final RandomAccessFile save = reader;
                reader = new RandomAccessFile(file, "r");
                // At this point, we're sure that the old file is rotated
                // Finish scanning the old file and then we'll start with the new one
                readLines(save);
                closeQuietly(save);
                sendMessage();
                LastRotateRequest request = new LastRotateRequest(
                        topicMeta.getTopic(),
                        source,
                        topicMeta.getRollPeriod());
                TransferWrap wrap = new TransferWrap(request);
                wrap.write(channel);
            } catch (IOException e) {
                LOG.error("Oops, got an exception:", e);
                closeQuietly(reader);
                closeChannelQuietly(channel);
                LOG.debug("process rotate failed, stop.");
                stop();
                agent.reportFailure(topicMeta.getMetaKey(), source, Util.getTS());
            }
        }
        
        public void process() {
            try {
                readLines(reader);
            } catch (IOException e) {
                LOG.error("Oops, process read lines fail:", e);
                closeQuietly(reader);
                closeChannelQuietly(channel);
                LOG.debug("process failed, stop.");
                stop();
                agent.reportFailure(topicMeta.getMetaKey(), source, Util.getTS());
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
                        if (accept && lineBuf.size() != 0) {
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
        
        private void sendMessage() throws IOException {
            messageBuffer.flip();
            ByteBufferMessageSet messages = new ByteBufferMessageSet(messageBuffer.slice());
            ProduceRequest request = new ProduceRequest(topicMeta.getTopic(), source, messages);
            TransferWrap wrap = new TransferWrap(request);
            wrap.write(channel);
            messageBuffer.clear();
            messageNum = 0;
        }
        
        private void handleLine(byte[] line) throws IOException {
            
            Message message = new Message(line); 
            
            if (messageNum >= 30 || message.getSize() > messageBuffer.remaining()) {
                sendMessage();
            }
            
            message.write(messageBuffer);
            messageNum++;
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
        
        private void closeChannelQuietly(SocketChannel channel) {
            Socket socket = channel.socket();
            try {
                if (socket != null) {
                    channel.socket().shutdownOutput();
                }
            } catch (IOException e1) {
            }
            try {
                channel.close();
            } catch (IOException e) {
            }
            try {
                if (socket != null) {
                    socket.close();
                    socket = null;
                }
            } catch (IOException e) {
            }
        }
    }
}

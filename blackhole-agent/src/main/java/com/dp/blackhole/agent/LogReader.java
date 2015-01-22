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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.agent.persist.IState;
import com.dp.blackhole.agent.persist.LocalState;
import com.dp.blackhole.agent.persist.Record;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.protocol.data.HaltRequest;
import com.dp.blackhole.protocol.data.ProduceRequest;
import com.dp.blackhole.protocol.data.RegisterRequest;
import com.dp.blackhole.protocol.data.RollRequest;
import com.dp.blackhole.storage.ByteBufferMessageSet;
import com.dp.blackhole.storage.Message;

public class LogReader implements Runnable {
    private static final Log LOG = LogFactory.getLog(LogReader.class);
    private static final int IN_BUF = 1024 * 8;
    private static final int SYS_MAX_LINE_SIZE = 1024 * 512;
    public static final long EOF = -1L;
    public static final long BOF = 0L;

    private TopicMeta topicMeta;
    private Agent agent;
    private String source;
    private String broker;
    private int brokerPort;
    private Socket socket;
    private EventWriter eventWriter;
    private AtomicReference<LogStatus> currentLogStatus;
    private final IState state;
    
    public LogReader(Agent agent, String localhost, String broker, int port, TopicMeta topicMeta, String snapshotPersistDir) {
        this.agent = agent;
        this.source =  Util.getSource(localhost, topicMeta.getInstanceId());
        this.broker = broker;
        this.brokerPort = port;
        this.topicMeta = topicMeta;
        this.currentLogStatus = new AtomicReference<LogReader.LogStatus>(LogStatus.NONE);
        this.state = new LocalState(snapshotPersistDir, topicMeta);
    }
    
    public IState getState() {
        return state;
    }
    
    public LogStatus getCurrentLogStatus() {
        return currentLogStatus.get();
    }
    
    public void resetCurrentLogStatus() {
        currentLogStatus.set(LogStatus.NONE);
    }

    public void doFileAppend() {
        currentLogStatus.compareAndSet(LogStatus.APPEND, LogStatus.APPEND);
    }
    
    public void doFileAppendForce() {
        currentLogStatus.set(LogStatus.APPEND);
    }
    
    public void beginLogRotate() {
        currentLogStatus.set(LogStatus.ROTATE);
    }
    
    public void beginLastLogRotate() {
        currentLogStatus.set(LogStatus.HALT);
    }
    
    public void beginRollAttempt() {
        currentLogStatus.set(LogStatus.ROLL);
    }
    
    public void finishLogRotate() {
        currentLogStatus.compareAndSet(LogStatus.ROTATE, LogStatus.APPEND);
    }
    
    public void finishHalt() {
        currentLogStatus.compareAndSet(LogStatus.HALT, LogStatus.NONE);
    }
    
    public void finishRoll() {
        currentLogStatus.compareAndSet(LogStatus.ROLL, LogStatus.APPEND);
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
        agent.getListener().unregisterLogReader(topicMeta.getTailFile(), this);
    }

    @Override
    public void run() {
        try {
            LOG.info("Log reader for " + topicMeta + " running...");
            
            File tailFile = new File(topicMeta.getTailFile());
            eventWriter = new EventWriter(state, tailFile, topicMeta.getMaxLineSize(), topicMeta.getReadInterval());
            eventWriter.initializeRemoteConnection();
            eventWriter.start();
            
            if (!agent.getListener().registerLogReader(topicMeta.getTailFile(), this)) {
                throw new IOException("Failed to register a log reader for " + topicMeta.getTopicId() 
                        + " with " + topicMeta.getTailFile() + ", thread will not run.");
            }
            RollTrigger rollTrigger = new RollTrigger(topicMeta, this);
            rollTrigger.trigger();
        } catch (Throwable t) {
            LOG.error("Oops, got an exception", t);
            agent.reportFailure(topicMeta.getTopicId(), source, Util.getTS());
        }
    }
    
    private enum LogStatus {
        NONE,
        APPEND,
        ROLL,
        ROTATE,
        HALT;
    }
    
    class EventWriter extends Thread {
        private final IState state;
        private final File file;
        private final byte inbuf[] = new byte[IN_BUF];
        private SocketChannel channel;
        private RandomAccessFile reader;
        private final int maxLineSize;
        private final ByteArrayOutputStream lineBuf;
        private boolean accept;
        private ByteBuffer messageBuffer;
        private int messageNum;
        private volatile boolean running = true;
        private long readInterval;
        
        public EventWriter(final IState state, final File file, int maxLineSize, long readInterval) throws IOException {
            this.setDaemon(true);
            this.setName("EventWriter-" + file.getName());
            this.state = state;
            this.file = file;
            if (maxLineSize > SYS_MAX_LINE_SIZE) {
                this.maxLineSize = SYS_MAX_LINE_SIZE;
            } else {
                this.maxLineSize = maxLineSize;
            }
            this.readInterval = readInterval;
            this.reader = new RandomAccessFile(file, "r");
            this.lineBuf = new ByteArrayOutputStream(maxLineSize);
            this.accept = true;
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
        
        public void initializeRemoteConnection() throws IOException {
            messageBuffer = ByteBuffer.allocate(512 * 1024);
            channel = SocketChannel.open();
            channel.connect(new InetSocketAddress(broker, brokerPort));
            socket = channel.socket();
            LOG.info(topicMeta.getTopicId() + " connected broker: " + broker + ":" + brokerPort);
            doStreamReg();
        }

        @Override
        public void run() {
            long rollPeriod = topicMeta.getRollPeriod();
            long rotatePeriod = topicMeta.getRotatePeriod();
            long resumeRollTs = Util.getLatestRollTsUnderTimeBuf(
                    Util.getTS(),
                    rollPeriod,
                    ParamsKey.DEFAULT_CLOCK_SYNC_BUF_MILLIS);
            
            restoreMissingRotationRecords(rollPeriod, rotatePeriod, resumeRollTs);

            // record the offset of agent resuming
            try {
                long fileLength = reader.length();
                //getOffsetOfLastLineHeader() will return the offset(from 0) of beginning of line, 
                //so minus 1 is back to the end offset of previous line
                //If getOffsetOfLastLineHeader() return 0, it declare that find a new unreaded file.
                long endOffset = Util.getOffsetOfLastLineHeader(reader, fileLength) - 1;
                state.record(Record.RESUME, resumeRollTs, endOffset);
            } catch (IOException e) {
                LOG.error("EventWriter thread can not start.", e);
                running = false;
            }
            while (running) {
                try {
                    switch (currentLogStatus.get()) {
                    case APPEND:
                        process();
                        Thread.sleep(readInterval);
                        break;
                    case ROTATE:
                        processRotate();
                        finishLogRotate();
                        break;
                    case HALT:
                        processHalt();
                        finishHalt();
                        break;
                    case ROLL:
                        processRoll();
                        finishRoll();
                        break;
                    case NONE:
                        Thread.sleep(1000);
                        break;
                    default:
                        throw new AssertionError("Undefined log status.");
                    }
                } catch (Throwable t) {
                    running = false;
                    LOG.error("exception catched when processing event", t);
                }
            }
        }

        /**
         * restore some missing ROTATE records before RESUME 
         * if agent crash across at least one period of rotation
         * @param rollPeriod
         * @param rotatePeriod
         * @param resumeRollTs
         */
        public void restoreMissingRotationRecords(long rollPeriod,
                long rotatePeriod, long resumeRollTs) {
            Record lastRollRecord = state.retriveLastRollRecord();
            if (lastRollRecord != null) {
                long firstMissRotaeRollTs = Util.getNextRollTs(lastRollRecord.getRollTs(), rotatePeriod) - rollPeriod * 1000;
                if (firstMissRotaeRollTs > lastRollRecord.getRollTs() && firstMissRotaeRollTs <= resumeRollTs) {
                    state.record(Record.ROTATE, firstMissRotaeRollTs, EOF);
                }
                int restMissRotateRollCount = Util.getMissRotateRollCount(firstMissRotaeRollTs, resumeRollTs, rotatePeriod);
                for (int i = 0; i < restMissRotateRollCount; i++) {
                    Record lastRotateRecord = state.retriveLastRollRecord();
                    long missRotateRollTs = Util.getNextRollTs(lastRotateRecord.getRollTs(), rotatePeriod) + (rotatePeriod  - rollPeriod) * 1000;
                    LOG.debug("find miss rotate roll ts " + missRotateRollTs);
                    state.record(Record.ROTATE, missRotateRollTs, EOF);
                }
            }
        }
        
        public void processRoll() {
            try {
                long previousRollEndOffset = readLines(reader) - 1; //minus 1 points out the previous ROLL end offset
                sendMessage();
                //do not handle log rotation any more when dying
                if (topicMeta.isDying()) {
                    return;
                }
                Long rollTs = Util.getLatestRollTsUnderTimeBuf(
                        Util.getTS(),
                        topicMeta.getRollPeriod(),
                        ParamsKey.DEFAULT_CLOCK_SYNC_BUF_MILLIS);
                //record snapshot
                state.record(Record.ROLL, rollTs, previousRollEndOffset);

                //send roll request to broker
                RollRequest request = new RollRequest(
                        topicMeta.getTopic(),
                        source,
                        topicMeta.getRollPeriod());
                TransferWrap wrap = new TransferWrap(request);
                wrap.write(channel);
            } catch (Exception e) {
                LOG.error("process roll attempt failed.", e);
                //TODO what should I do when exception occur
            }
        }
        
        public void processRotate() {
            try {
                final RandomAccessFile save = reader;
                reader = new RandomAccessFile(file, "r");
                // At this point, we're sure that the old file is rotated
                // Finish scanning the old file and then we'll start with the new one
                long previousRotateEndOffset = readLines(save) - 1;   //minus 1 points out the end offset of old file
                closeQuietly(save);
                sendMessage();
                //do not handle log rotation any more when dying
                if (topicMeta.isDying()) {
                    return;
                }
                
                Long rollTs = Util.getLatestRollTsUnderTimeBuf(
                        Util.getTS(),
                        topicMeta.getRollPeriod(),
                        ParamsKey.DEFAULT_CLOCK_SYNC_BUF_MILLIS);
                //record snapshot
                state.record(Record.ROTATE, rollTs, previousRotateEndOffset);
                
                //send roll request to broker
                RollRequest request = new RollRequest(
                        topicMeta.getTopic(),
                        source,
                        topicMeta.getRollPeriod());
                TransferWrap wrap = new TransferWrap(request);
                wrap.write(channel);
            } catch (Exception e) {
                LOG.error("Oops, got an exception:", e);
                closeQuietly(reader);
                closeChannelQuietly(channel);
                LOG.debug("process rotate failed, stop.");
                LogReader.this.stop();
                agent.reportFailure(topicMeta.getTopicId(), source, Util.getTS());
            }
        }
        
        public void processHalt() {
            try {
                final RandomAccessFile save = reader;
                reader = new RandomAccessFile(file, "r");
                // At this point, we're sure that the old file is rotated
                // Finish scanning the old file and then we'll start with the new one
                readLines(save);
                closeQuietly(save);
                sendMessage();
                HaltRequest request = new HaltRequest(
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
                LogReader.this.stop();
                agent.reportFailure(topicMeta.getTopicId(), source, Util.getTS());
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
                LogReader.this.stop();
                agent.reportFailure(topicMeta.getTopicId(), source, Util.getTS());
            }
        }
        
        /**
         * Read new lines.
         *
         * @param reader The file to read
         * @return The new position after the lines have been read
         * @throws java.io.IOException if an I/O error occurs.
         */
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

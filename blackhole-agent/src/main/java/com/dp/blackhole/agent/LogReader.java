package com.dp.blackhole.agent;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.agent.persist.IRecoder;
import com.dp.blackhole.agent.persist.LocalRecorder;
import com.dp.blackhole.agent.persist.Record;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;

public class LogReader implements Runnable {
    private static final Log LOG = LogFactory.getLog(LogReader.class);
    public static final long END_OFFSET_OF_FILE = -1L;
    public static final long BEGIN_OFFSET_OF_FILE = 0L;
    private enum ReaderState {NEW, UNREGISTERED, REGISTERED, SENDER_ASSIGNED, STOPPED}
    private static final int IN_BUF = 1024 * 8;
    private static final int SYS_MAX_LINE_SIZE = 1024 * 512;
    private final byte inbuf[] = new byte[IN_BUF];
    
    private Agent agent;
    private TopicMeta meta;
    private RemoteSender sender;
    private LogFSM logFSM;
    private AtomicReference<ReaderState> currentReaderState;
    private final IRecoder recoder;
    private RandomAccessFile reader;
    private final File tailFile;
    private volatile boolean running = true;
    private final ByteArrayOutputStream lineBuf;
    private boolean accept;
    private final int maxLineSize;
    private RollTrigger rollTrigger;
    
    public LogReader(Agent agent, TopicMeta meta, String snapshotPersistDir) {
        this.agent = agent;
        this.meta = meta;
        this.currentReaderState = new AtomicReference<ReaderState>(ReaderState.NEW);
        this.logFSM = new LogFSM();
        this.recoder = new LocalRecorder(snapshotPersistDir, meta);
        if (meta.getMaxLineSize() > SYS_MAX_LINE_SIZE) {
            this.maxLineSize = SYS_MAX_LINE_SIZE;
        } else {
            this.maxLineSize = meta.getMaxLineSize();
        }

        this.lineBuf = new ByteArrayOutputStream(maxLineSize);
        this.tailFile = new File(meta.getTailFile());
        this.accept = true;
    }
    
    public LogFSM getLogFSM() {
        return logFSM;
    }

    public RemoteSender getSender() {
        return sender;
    }

    public void setSender(RemoteSender sender) {
        this.sender = sender;
    }

    public IRecoder getRecoder() {
        return recoder;
    }
    
    public boolean register() {
        return agent.getListener().registerLogReader(meta.getTailFile(), logFSM);
    }

    public void unregister() {
        agent.getListener().unregisterLogReader(meta.getTailFile(), logFSM);
    }
    
    public void stop() {
        running = false;
    }
    
    public void assignSender(RemoteSender newSender) {
        setSender(newSender);
        ReaderState oldReaderState = currentReaderState.getAndSet(ReaderState.SENDER_ASSIGNED);
        LOG.debug("Assign sender: " + oldReaderState.name() + " -> SENDER_ASSIGNED");
    }
    
    private void reassignSender() {
        currentReaderState.compareAndSet(ReaderState.SENDER_ASSIGNED, ReaderState.REGISTERED);
        int reassignDelay = sender.getReassignDelaySeconds();
        sender.close();
        agent.reportRemoteSenderFailure(meta.getTopicId(), meta.getSource(), Util.getTS(), reassignDelay);
    }
    
    @Override
    public void run() {
        LOG.info("Log reader for " + meta + " running...");
        try {
            this.reader = new RandomAccessFile(tailFile, "r");
            long resumeRollTs = Util.getLatestRollTsUnderTimeBuf(
                    Util.getTS(),
                    meta.getRollPeriod(),
                    ParamsKey.DEFAULT_CLOCK_SYNC_BUF_MILLIS);
            restoreMissingRotationRecords(meta.getRollPeriod(), meta.getRotatePeriod(), resumeRollTs);

            //record the offset of agent resuming
            long fileLength = reader.length();
            //minus 1 is back to the end offset of previous line
            //so if seekLastLineHeader() return 0, it declare that it's a new unread file.
            long endOffset = Util.seekLastLineHeader(reader, fileLength) - 1;
            getRecoder().record(Record.RESUME, resumeRollTs, endOffset);
        } catch (IOException e) {
            LOG.error("Oops, resume fail", e);
            closeQuietly(reader);
            agent.reportLogReaderFailure(meta.getTopicId(), meta.getSource(), Util.getTS());
            return;
        }
        
        if(currentReaderState.get() == ReaderState.NEW) {
            if (!register()) {
                LOG.error("Failed to register a log reader for " + meta.getTopicId() 
                        + " with " + meta.getTailFile() + "log reader will exit.");
                closeQuietly(reader);
                agent.reportLogReaderFailure(meta.getTopicId(), meta.getSource(), Util.getTS());
                return;
            }
            rollTrigger = new RollTrigger(meta, logFSM);
            rollTrigger.trigger();
            currentReaderState.compareAndSet(ReaderState.NEW, ReaderState.REGISTERED);
        }
        
        // main loop
        loop();
        
        // after main loop, log reader thread will be shutdown and resources will be released 
        currentReaderState.set(ReaderState.STOPPED);
        closeQuietly(reader);
        sender.close();
        sender = null;
        unregister();
        rollTrigger.stop();
        LOG.warn("terminate log reader " + meta.getTopicId() + ", resources released.");
    }

    private void loop() {
        while (running) {
            try {
                switch (logFSM.getCurrentLogStatus()) {
                case NEW:
                    Thread.sleep(1000);
                    break;
                case APPEND:
                    process();
                    Thread.sleep(meta.getReadInterval());
                    break;
                case ROLL:
                case LAST_ROLL:
                    processRoll();
                    break;
                case ROTATE:
                    processRotate();
                    break;
                case FINISHED:
                    //Do not end the loop here, it will terminate the thread and release all resources.
                    //Once the socket (in RemoteSender) closed, broker will lose the agent connection 
                    //and throw EOFExeception instead of uploading data.
                    //So, it should not break the loop until agent receives CLEAN event. 
                    Thread.sleep(1000);
                    break;
                default:
                    throw new AssertionError("Undefined log status.");
                }
            } catch (Throwable t) {
                running = false;
                LOG.error("exception catched when processing log reader loop.", t);
                agent.reportLogReaderFailure(meta.getTopicId(), meta.getSource(), Util.getTS());
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
        Record lastRollRecord = getRecoder().retriveLastRollRecord();
        if (lastRollRecord != null) {
            long firstMissRotaeRollTs = Util.getNextRollTs(lastRollRecord.getRollTs(), rotatePeriod) - rollPeriod * 1000;
            if (firstMissRotaeRollTs > lastRollRecord.getRollTs() && firstMissRotaeRollTs <= resumeRollTs) {
                getRecoder().record(Record.ROTATE, firstMissRotaeRollTs, END_OFFSET_OF_FILE);
            }
            int restMissRotateRollCount = Util.getMissRotateRollCount(firstMissRotaeRollTs, resumeRollTs, rotatePeriod);
            for (int i = 0; i < restMissRotateRollCount; i++) {
                Record lastRotateRecord = getRecoder().retriveLastRollRecord();
                long missRotateRollTs = Util.getNextRollTs(lastRotateRecord.getRollTs(), rotatePeriod) + (rotatePeriod  - rollPeriod) * 1000;
                LOG.debug("find miss rotate roll ts " + missRotateRollTs);
                getRecoder().record(Record.ROTATE, missRotateRollTs, END_OFFSET_OF_FILE);
            }
        }
    }
    
    public void process() {
        try {
            if (currentReaderState.get() == ReaderState.SENDER_ASSIGNED) {
                readLines(reader);
            }
        } catch (SocketException e) {
            LOG.error("Fail while sending message, reassign sender", e);
            reassignSender();
        } catch (IOException e) {
            LOG.error("Oops, got an exception:", e);
            throw new RuntimeException("log reader loop terminate, prepare to reboot log reader.");
        }
    }
    
    public void processRoll() {
        long rollEndOffset = END_OFFSET_OF_FILE;
        try {
            rollEndOffset = reader.getFilePointer() - 1; //minus 1 points out the previous ROLL end offset
            //do not handle log rotation any more when dying
        } catch (IOException e) {
            LOG.error("Oops, got an exception:", e);
            throw new RuntimeException("log reader loop terminate, prepare to reboot log reader.");
        } finally {
            Long rollTs;
            if (meta.isDying()) {
                rollTs = Util.getCurrentRollTsUnderTimeBuf(
                        Util.getTS(),
                        meta.getRollPeriod(),
                        ParamsKey.DEFAULT_CLOCK_SYNC_BUF_MILLIS);
            } else {
                rollTs = Util.getLatestRollTsUnderTimeBuf(
                        Util.getTS(),
                        meta.getRollPeriod(),
                        ParamsKey.DEFAULT_CLOCK_SYNC_BUF_MILLIS);
            }
            //record snapshot
            getRecoder().record(Record.ROLL, rollTs, rollEndOffset);
        }
        try {
            if (currentReaderState.get() == ReaderState.SENDER_ASSIGNED) {
                sender.sendMessage();   //clear buffer
                if (meta.isDying()) {
                    sender.sendHaltRequest();
                } else {
                    sender.sendRollRequest();
                }
            }
        } catch (Exception e) {
            LOG.error("Fail while sending roll request, reassign sender", e);
            reassignSender();
        } finally {
            if (meta.isDying()) {
                logFSM.finishLastRoll();
            } else {
                logFSM.finishRoll();
            }
        }
    }
    
    public void processRotate() {
        final RandomAccessFile save = reader;
        long previousRotateEndOffset = END_OFFSET_OF_FILE;
        try {
            try {
                reader = new RandomAccessFile(tailFile, "r");
                // At this point, we're sure that the old file is rotated
                // Finish scanning the old file and then we'll start with the new one
                // minus 1 points out the end offset of old file
                previousRotateEndOffset = readLines(save) - 1;
            } catch (SocketException e) {
                throw e;
            } catch (IOException e) {
                LOG.error("Oops, got an exception:", e);
                throw new RuntimeException("log reader loop terminate, prepare to reboot log reader.");
            } finally {
                closeQuietly(save);
                Long rollTs = Util.getLatestRollTsUnderTimeBuf(
                        Util.getTS(),
                        meta.getRollPeriod(),
                        ParamsKey.DEFAULT_CLOCK_SYNC_BUF_MILLIS);
                //record snapshot
                getRecoder().record(Record.ROTATE, rollTs, previousRotateEndOffset);
            }
            
            if (currentReaderState.get() == ReaderState.SENDER_ASSIGNED) {
                //send left message force
                sender.sendMessage();
                //send roll request to broker
                sender.sendRollRequest();
            }
        } catch (IOException e) {
            LOG.error("Fail while sending rotate request, reassign sender", e);
            reassignSender();
        } finally {
            logFSM.finishLogRotate();
        }
    }

    /**
     * Read new lines.
     *
     * @param reader The file to read
     * @return The new position after the lines have been read
     * @throws java.io.IOException if an I/O error occurs.
     */
    private long readLines(RandomAccessFile reader) throws SocketException, IOException {
        long pos = reader.getFilePointer();
        long rePos = pos; // position to re-read
        int num;
        while ((num = reader.read(inbuf)) != -1) {
            for (int i = 0; i < num; i++) {
                final byte ch = inbuf[i];
                switch (ch) {
                case '\n':
                    if (accept && lineBuf.size() != 0) {
                        try {
                            sender.cacahAndSendLine(lineBuf.toByteArray());
                        } catch (IOException e) {
                            throw new SocketException("send line fail");
                        }
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

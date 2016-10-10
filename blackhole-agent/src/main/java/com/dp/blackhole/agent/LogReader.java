package com.dp.blackhole.agent;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.ParamsKey.TopicConf;

public class LogReader implements Runnable {
    private static final Log LOG = LogFactory.getLog(LogReader.class);
    public static final long END_OFFSET_OF_FILE = -1L;
    public static final long BEGIN_OFFSET_OF_FILE = 0L;
    private enum ReaderState {UNASSIGNED, ASSIGNED, STOPPED}
    private static final int IN_BUF = 1024 * 8;
    private final byte inbuf[] = new byte[IN_BUF];
    
    private Agent agent;
    private AgentMeta meta;
    private volatile RemoteSender sender;
    private LogFSM logFSM;
    private AtomicReference<ReaderState> currentReaderState;
    private RandomAccessFile reader;
    private final File tailFile;
    private volatile boolean running = true;
    private final ByteArrayOutputStream lineBuf;
    private boolean accept;
    private final int maxLineSize;
    private long currentRotation;
    private OffsetInfo infoSaved = null;
    
    public LogReader(Agent agent, AgentMeta meta) {
        this.agent = agent;
        this.meta = meta;
        this.currentReaderState = new AtomicReference<ReaderState>(ReaderState.UNASSIGNED);
        this.logFSM = new LogFSM();
        this.maxLineSize = meta.getMaxLineSize();
        this.currentRotation = setCurrentRotation();
        this.lineBuf = new ByteArrayOutputStream(maxLineSize);
        this.tailFile = new File(meta.getTailFile());
        this.accept = true;
        this.infoSaved = new OffsetInfo(0L, meta.getTopic(), meta.getSource());
    }
    
    public LogFSM getLogFSM() {
        return logFSM;
    }

    public RemoteSender getSender() {
        return sender;
    }

    public long getCurrentRotation() {
        return currentRotation;
    }
    
    public void setCurrentRotation(long rotateTs) {
        this.currentRotation = rotateTs;
    }

    public File getTailFile() {
        return tailFile;
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
    
    public void assignSender(RemoteSender sender) {
        this.sender = sender;
        try {
            this.sender.setOffsetInfo(infoSaved);
        } catch (IOException e) {
            LOG.error("Fail while re-sending message, reassign sender", e);
            reassignSender(sender);
            return;
        }
        ReaderState oldReaderState = currentReaderState.getAndSet(ReaderState.ASSIGNED);
        LOG.info("Assign sender: " + oldReaderState.name() + " -> " + ReaderState.ASSIGNED.name());
    }
    
    private void reassignSender(RemoteSender sender) {
        sender.close();
        if (currentReaderState.compareAndSet(ReaderState.ASSIGNED, ReaderState.UNASSIGNED)) {
            LOG.info("remove sender: " + ReaderState.ASSIGNED.name() + " -> " + currentReaderState.get().name());
            int reassignDelay = sender.getReassignDelaySeconds();
            //must unregister from ConnectionChecker before re-assign
            agent.getLingeringSender().unregister(sender);
            agent.reportRemoteSenderFailure(meta.getTopicId(), meta.getSource(), Util.getTS(), reassignDelay);
        }
    }
    
    long setCurrentRotation() {
        long currentTs = Util.getTS();
        long currentRotation = Util.getCurrentRotationUnderTimeBuf(
                currentTs,
                meta.getRotatePeriod(),
                ParamsKey.DEFAULT_CLOCK_SYNC_BUF_MILLIS);
        setCurrentRotation(currentRotation);
        LOG.info("Set current rotation " + currentRotation + " for " + meta.getTopic());
        return currentRotation;
    }
    
    @Override
    public void run() {
        LOG.info("Log reader for " + meta + " running...");
        try {
            this.reader = openFile();
            long tailPosition = meta.getTailPosition();
            if (tailPosition == TopicConf.FILE_TAIL) {
                tailPosition = Util.seekLastLineHeader(reader, reader.length());
            } else if (tailPosition == TopicConf.FILE_HEAD) {
                //do nothing
            } else {
                //TODO tail from specified offset that should point out the tail file.
                // Fix it in future and now tail from tail instead
                tailPosition = Util.seekLastLineHeader(reader, reader.length());
            }
            LOG.info("tail " + tailFile + " from " + tailPosition);
        } catch (IOException e) {
            LOG.error("Oops, resume fail", e);
            closeFile(reader);
            agent.reportLogReaderFailure(meta.getTopicId(), meta.getSource(), Util.getTS());
            currentReaderState.set(ReaderState.STOPPED);
            return;
        }
        
        if (!register()) {
            LOG.error("Failed to register a log reader for " + meta.getTopicId() 
                    + " with " + meta.getTailFile() + "log reader will exit.");
            closeFile(reader);
            agent.reportLogReaderFailure(meta.getTopicId(), meta.getSource(), Util.getTS());
            currentReaderState.set(ReaderState.STOPPED);
            return;
        }
        
        // main loop
        loop();
        
        // after main loop, log reader thread will be shutdown and resources will be released 
        currentReaderState.set(ReaderState.STOPPED);
        closeFile(reader);
        sender.close();
        sender = null;
        unregister();
        LOG.warn("terminate log reader " + meta.getTopicId() + ", resources released.");
    }

    private RandomAccessFile openFile() throws FileNotFoundException {
        RandomAccessFile raf = new RandomAccessFile(tailFile, "r");
        return raf;
    }

    private void closeFile(RandomAccessFile raf) {
        closeQuietly(raf);
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
                case ROTATE:
                    processRotate();
                    break;
                case HALT:
                    processHalt();
                    break;
                case FINISHED:
                    //TODO should refactor
                    //Do not end the loop here, it will terminate the thread and release all resources.
                    //Once the socket (in RemoteSender) closed, broker will lose the agent connection 
                    //and throw EOFExeception instead of uploading data.
                    //So, it should not break the loop until agent receives CLEAN event. 
                    LOG.info("handle finish, just sleep 60 seconds to wait broker upload, then stop self thread "
                            + Thread.currentThread().getName());
                    Thread.sleep(60000);
                    running = false;
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
    
    public void process() {
        try {
            if (currentReaderState.get() == ReaderState.ASSIGNED) {
                readLines(reader);
            }
        } catch (SocketException e) {
            LOG.error("Fail while sending message, reassign sender", e);
            reassignSender(sender);
        } catch (IOException e) {
            LOG.error("Oops, got an exception:", e);
            throw new RuntimeException("log reader loop terminate, prepare to reboot log reader.");
        }
    }
    
    public void processRotate() {
        if (currentReaderState.get() != ReaderState.ASSIGNED) {
            LOG.warn("RemoteSender not ready for " + meta.getTopicId());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error("Interrupted!", e);
            }
            return;
        }
        
        try {
            final RandomAccessFile save = reader;
            try {
                this.reader = openFile();
                readLines(save);
            } catch (SocketException e) {
                throw e;
            } catch (IOException e) {
                LOG.error("Oops, got an exception:", e);
                throw new RuntimeException("log reader loop terminate, prepare to reboot log reader.");
            } finally {
                closeFile(save);
            }
            
            if (currentReaderState.get() == ReaderState.ASSIGNED) {
                //send left message force
                sender.sendMessage();
                //send roll request to broker
                sender.sendRollRequest();
            }
        } catch (IOException e) {
            LOG.error("Fail while sending rotate request, reassign sender", e);
            reassignSender(sender);
        } finally {
            logFSM.finishLogRotate();
        }
    }
    
    public void processHalt() {
        try {
            try {
                readLines(reader);
            } catch (SocketException e) {
                throw e;
            } catch (IOException e) {
                LOG.error("Oops, got an exception:", e);
            } finally {
                closeFile(reader);
            }
            
            //send left message force
            sender.sendMessage();
            sender.sendHaltRequest();
        } catch (IOException e) {
            LOG.error("Fail while sending halt request, abort resender", e);
        } finally {
            logFSM.finishHalt();
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
        if (!sender.isActive()) {
            throw new SocketException("send is not active");
        }
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
                            lineBuf.reset();
                            reader.seek(rePos);
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

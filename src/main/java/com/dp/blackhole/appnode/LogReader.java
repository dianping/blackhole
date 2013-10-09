package com.dp.blackhole.appnode;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.charset.Charset;

import net.contentobjects.jnotify.IJNotify;
import net.contentobjects.jnotify.JNotifyException;
import net.contentobjects.jnotify.JNotifyListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.conf.ConfigKeeper;

public class LogReader implements Runnable{
    private static final Log LOG = LogFactory.getLog(LogReader.class);
    
    public static final int FILE_CREATED    = 0x1;
    public static final int FILE_DELETED    = 0x2;
    public static final int FILE_MODIFIED   = 0x4;
    public static final int FILE_RENAMED    = 0x8;
    public static final int FILE_ANY        = FILE_CREATED | FILE_DELETED | FILE_MODIFIED | FILE_RENAMED;
    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    private String  collectorServer;
    private int port;
    private AppLog appLog;
    private File tailFile;
    private Appnode node;
    private int bufSize;
    private Socket socket;
    private DataOutputStream out;
    private static IJNotify instance;
    private int parentWatchID, watchId;
    private volatile boolean run = true;
    
    static {
        try {
            instance = (IJNotify) Class.forName("net.contentobjects.jnotify.linux.JNotifyAdapterLinux").newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public LogReader(Appnode node, String collectorServer, int port, AppLog appLog) {
        this.node = node;
        this.collectorServer = collectorServer;
        this.port = port;
        this.appLog =appLog;
        this.bufSize = ConfigKeeper.configMap.get(appLog.getAppName()).getInteger(ParamsKey.Appconf.BUFFER_SIZE);
    }

    public void initialize() throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        tailFile = new File(appLog.getTailFile()).getCanonicalFile();
        if (tailFile == null) {
            throw new FileNotFoundException("tail file not found");
        }
        socket = new Socket(collectorServer, port);
        out = new DataOutputStream(socket.getOutputStream());

        AgentProtocol protocol = new AgentProtocol();
        AgentHead head = protocol.new AgentHead();
        
        head.type = AgentProtocol.STREAM;
        head.app = appLog.getAppName();
        head.peroid = ConfigKeeper.configMap.get(appLog.getAppName()).getLong(ParamsKey.Appconf.ROLL_PERIOD);
        protocol.sendHead(out, head);
    }

    public void stop() {
        this.run = false;
        try {
            instance.removeWatch(watchId);
            instance.removeWatch(parentWatchID);
            Thread.currentThread().interrupt();
            if (socket != null && !socket.isClosed()) {
                socket.close();
                socket = null;
            }
        } catch (IOException e) {
            LOG.warn("Warnning, clean fail:", e);
        }
    }

    private boolean getRun() {
        return run;
    }
    @Override
    public void run() {
        RandomAccessFile reader = null;
        try {
            initialize();
            String watchPath = tailFile.getCanonicalPath();
            String parentWatchPath = tailFile.getParentFile().getCanonicalPath();
            reader = new RandomAccessFile(tailFile, "r");
            EventWriter eventWriter = new EventWriter(reader, tailFile, bufSize);
            Listener listener = new Listener(watchPath, eventWriter);
            watchId = instance.addWatch(watchPath, FILE_MODIFIED, false, listener);
            LOG.info("Monitoring tail file " + watchPath + " \"FILE_MODIFIED\" first.");
            parentWatchID = instance.addWatch(parentWatchPath, FILE_CREATED, false, listener);
            LOG.info("Monitoring parent path " + parentWatchPath + " \"FILE_CREATE\" for rotate.");
            while (getRun()) {
                Thread.sleep(5000);
            }
        } catch (final InterruptedException e) {
//            LOG.info("Interrputed.", e);
        } catch (FileNotFoundException e) {
            LOG.error("Got an exception", e);
            node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
        } catch (IOException e) {
            LOG.error("This thread stop! ", e);
            node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
        } catch (Exception e) {
            LOG.error("Oops, got an exception:" , e);
            node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
        } finally {
            stop();
        }
    }
    
    class Listener implements JNotifyListener {
        private String watchPath;
        private EventWriter eventWriter;
        Listener(String watchPath, EventWriter eventWriter) {
            this.watchPath = watchPath;
            this.eventWriter = eventWriter;
        }
        
        @Override
        public void fileCreated(int wd, String rootPath, String name) {
            if (name.equals(watchPath.substring(watchPath.lastIndexOf('/') + 1))) {
                LOG.info("rotate detected " + rootPath + "/" + name);
                eventWriter.processRotate();
                try {
                    instance.removeWatch(watchId);//TODO review
                    watchId = instance.addWatch(watchPath, FILE_MODIFIED, false, this);
                    LOG.info("Re-monitoring "+ watchPath + " \"FILE_MODIFIED\" for rotate.");
                } catch (JNotifyException e) {
                    LOG.error("Add watch exception", e);
                }
            }
            else {
                LOG.warn("created " + rootPath + "/" + name +
                		" not equals watch file" + watchPath);
            }
        }

        @Override
        public void fileDeleted(int wd, String rootPath, String name) {
        }

        @Override
        public void fileModified(int wd, String rootPath, String name) {
            eventWriter.process();
        }

        @Override
        public void fileRenamed(int wd, String rootPath, String oldName,
                String newName) {
        }
    }
    
    class EventWriter {
        private final Charset cset;
        private final File file;
        private final byte inbuf[];
        private OutputStreamWriter writer;
        private RandomAccessFile reader;
        
        public EventWriter(final RandomAccessFile reader, final File file, final int bufSize) {
            this(reader, file, bufSize, DEFAULT_CHARSET);
        }
        public EventWriter(final RandomAccessFile reader, final File file, final int bufSize, Charset cset) {
            this.reader = reader;
            this.file = file;
            this.inbuf = new byte[bufSize];
            this.cset = cset;
            writer = new OutputStreamWriter(out);
        }
        
        public void processRotate() {
            try {
                writer.write('\n'); //make server easy to handle
                writer.flush();
                
                final RandomAccessFile save = reader;
                reader = new RandomAccessFile(file, "r");
                // At this point, we're sure that the old file is rotated
                // Finish scanning the old file and then we'll start with the new one
                readLines(save);
                closeQuietly(save);
            } catch (IOException e) {
                LOG.error("Oops, got an exception:", e);
                closeQuietly(reader);
                closeQuietly(writer);
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
        
        private void handleLine(String line) {
            try {
                writer.write(line);
                writer.write('\n'); //make server easy to handle
//                writer.flush();
            } catch (IOException e) {
                LOG.error("Oops, got an exception:", e);
                closeQuietly(reader);
                closeQuietly(writer);
                stop();
                node.reportFailure(appLog.getAppName(), node.getHost(), Util.getTS());
                
            }
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
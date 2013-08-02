package com.dp.blackhole.appnode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//TODO CAN NOT SET THIS TO LISTENER
public class LogReader implements Runnable{
    private static final Log LOG = LogFactory.getLog(LogReader.class);
    private final boolean isTailFromEnd = true;
    private Tailer tailer;
    private String  collectorServer;
    private int port;
    private AppLog appLog;
    private long delayMillis;
    private File tailFile;
    private LogTailerListener listener;
    private OutputStreamWriter writer;
    public LogReader(String collectorServer, int port, AppLog appLog, long delayMillis) {
        this.collectorServer = collectorServer;
        this.port = port;
        this.appLog =appLog;
        this.delayMillis = delayMillis;
    }

    public void initialize() throws FileNotFoundException {
        tailFile = new File(appLog.getTailFile());
        if (tailFile == null) {
            throw new FileNotFoundException("tail file not found");
        }
        listener = new LogTailerListener(tailFile.getAbsolutePath(), this);
        tailer = new Tailer(tailFile, listener, delayMillis, isTailFromEnd);
    }

    public void process(String line) {
        try {
            writer.write(line);
            writer.write('\n'); //make server easy to handle
            writer.flush();
            LOG.debug("client>" + line);
        } catch (IOException e) {
            //TODO retry app reg
            LOG.error("Oops, got an exception:", e);
        }
    }

    public void stop() {
        tailer.stop();
    }

    @Override
    public void run() {
        try {
            initialize();
            Socket server = new Socket(collectorServer, port);
            writer = new OutputStreamWriter(server.getOutputStream());
            tailer.run();
        } catch (FileNotFoundException e) {
            LOG.error("Got an exception", e);
        } catch (UnknownHostException e) {
            //TODO retry app reg
            LOG.error("Faild to build a socket with host:" 
                    + collectorServer + " port:" + port
                    + "This tailer thread stop! ", e);
        } catch (IOException e) {
            LOG.error("Faild to build a socket. " +
                    "This tailer thread stop! ", e);
        } catch (Exception e) {
            LOG.error("Oops, got an exception:" + e);
        }
    }
}
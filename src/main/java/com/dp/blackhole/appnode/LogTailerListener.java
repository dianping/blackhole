package com.dp.blackhole.appnode;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.conf.AppConfigurationConstants;
import com.dp.blackhole.conf.ConfigKeeper;

public class LogTailerListener implements TailerListener {
    public static final Log LOG = LogFactory.getLog(LogTailerListener.class);
    private String collectorServer;
    private AppLog appLog;
    private Socket server;
    private Tailer tailer;
    private OutputStreamWriter writer;
    public LogTailerListener(String collectorServer, AppLog appLog) {
        this.collectorServer = collectorServer;
        this.appLog = appLog;
    }

    /**
     * The tailer will call this method during construction,
     * giving the listener a method of stopping the tailer.
     * @param tailer the tailer.
     */
    public void init(Tailer tailer) {
        this.tailer = tailer;
        LOG.debug("listening the applog: " + appLog.getAppName());
        int port = ConfigKeeper.configMap.get(appLog.getAppName())
                .getInteger(AppConfigurationConstants.PORT);
        try {
            server = new Socket(collectorServer, port);
            writer = new OutputStreamWriter(server.getOutputStream());
        } catch (UnknownHostException e) {
            tailer.stop();
            LOG.error("Faild to build a socket with host:" 
                    + collectorServer + " port:" + port
            		+ "This tailer thread stop! ", e);
        } catch (IOException e) {
            tailer.stop();
            LOG.error("Faild to build a socket. " +
            		"This tailer thread stop! ", e);
        }
    }

    /**
     * This method is called if the tailed file is not found.
     */
    public void fileNotFound(){
        LOG.warn("File " + appLog.getTailFile() + " not found");
    }

    /**
     * Called if a file rotation is detected.
     * Insert a "" string to tail stream to distinguish 
     * different interval file like "trace.log.2013-07-11.12".
     * And, send a message APP_ROLL to supervisor 
     * which include file identify and its length.
     * 
     * This method is called before the file is reopened, and fileNotFound may
     * be called if the new file has not yet been created.
     */
    public void fileRotated() {
        handle("");
        LOG.info("File rotation is deteced.");
    }

    /**
     * Handles a line from a Tailer.
     * @param line the line.
     */
    public void handle(String line) {
        try {
            writer.write(line);
            writer.write('\n'); //make server easy to handle
            writer.flush();
            LOG.debug("client>" + line);
        } catch (IOException e) {
            LOG.error("Oops, got an exception:", e);
        }
    }

    /**
     * Handles an Exception .
     * @param ex the exception.
     */
    public void handle(Exception ex) {
        LOG.error("Oops, got an exception:", ex);
    }
}
package com.dp.blackhole.appnode;

import java.nio.charset.Charset;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LogTailerListener implements TailerListener {
    public static final Log LOG = LogFactory.getLog(LogTailerListener.class);
    private static final Charset charset = Charset.defaultCharset();
    private LogReader logReader;
    private String tailFile;
    private boolean fileExists;
    public LogTailerListener(String tailFile, LogReader logReader) {
        this.tailFile = tailFile;
        this.logReader = logReader;
        this.fileExists = false;
    }

    /**
     * The tailer will call this method during construction,
     * giving the listener a method of stopping the tailer.
     * @param tailer the tailer.
     */
    public void init(Tailer tailer) {
    }

    /**
     * This method is called if the tailed file is not found.
     */
    public void fileNotFound(){
        LOG.warn("File " + tailFile + " not found");
        fileExists = false;
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
        if (fileExists) {
            handle("");
            LOG.info("File " + tailFile+ " rotation is deteced.");
        }
    }

    /**
     * Handles a line from a Tailer.
     * @param line the line.
     */
    public void handle(String line) {
        fileExists = true;
        char[] chs = line.toCharArray();
        byte[] data = new byte[chs.length];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) chs[i];
        }
        line = new String(data, charset);
        logReader.process(line);
    }

    /**
     * Handles an Exception .
     * @param ex the exception.
     */
    public void handle(Exception ex) {
        LOG.error("Oops, got an exception:", ex);
    }
}
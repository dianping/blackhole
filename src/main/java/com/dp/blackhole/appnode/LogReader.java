package com.dp.blackhole.appnode;

import java.io.File;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class LogReader extends Tailer{
    public static final Log LOG = LogFactory.getLog(LogReader.class);
    public LogReader(Appnode appnode, AppLog appLog, boolean end) {
        super(new File(appLog.getTailFile()), new LogTailerListener(appLog), 100, end);
    }

    @Override
    public void run() {
        super.run();
    }
}
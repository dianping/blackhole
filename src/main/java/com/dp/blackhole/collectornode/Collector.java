package com.dp.blackhole.collectornode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;

public class Collector implements Runnable {

    private final static Log LOG = LogFactory.getLog(Collector.class);
    Socket socket;
    final String remoteAddress;
    final String app;
    OutputStreamWriter writer;
    BufferedReader streamIn;
    Collectornode node;
    String storagedir;
    File appending;
    long rollPeriod;
    SimpleDateFormat format;
    
    public Collector(Collectornode server, Socket s, String home, String appname, String host, long period) {
        node = server;
        socket = s;
        remoteAddress = Util.getRemoteHost(s);
        app = appname;
        rollPeriod = period;
        format = new SimpleDateFormat(Util.getFormatFromPeroid(period));
        storagedir = home+"/"+ app + "/" + remoteAddress;
        
        init();
    }
    
    public void init() {
        try {
            appending = node.getappendingFile(storagedir);
            writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(appending)));
            streamIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            LOG.error("error in init: ", e);
            handleIOException() ;
        }
    }
    
    @Override
    public void run() {
        String event;
        try {
            while ((event = streamIn.readLine()) != null) {
                if (event.length() != 0) {
                    writetofile(event);
                    emit(event);
                } else {
                    completefile();
                }
            }
            handleIOException();
        } catch (IOException e) {
            handleIOException();
        }
    }
    
    private void handleIOException() {
        node.reportFailure(this, app, remoteAddress, Util.getTS());
        close();
    }
    
    public void close() {
        try {
            if (streamIn != null) {
                streamIn.close();
            }
            if (writer != null) {
                writer.close();
            }
            socket.close();
        } catch (IOException e) {
        }
    }

    private void writetofile(String line) throws IOException {
        writer.write(line);
        writer.write('\n');
    }

    private void completefile() throws IOException {
        RollIdent rollIdent = getRollIdent();
        File rollFile = getRollFile(rollIdent);

        writer.close();
        if(!appending.renameTo(rollFile)) {
            LOG.error("rename to " + rollFile + " failed");
        }
        appending = node.getappendingFile(storagedir);
        writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(appending)));
        
        boolean success = node.registerfile(rollIdent, rollFile);
        if (success) {
            LOG.info("complete file: " + rollFile + ", roll " + rollIdent);
        }
    }

    private RollIdent getRollIdent() {
        Date time = new Date(Util.getClosestRollTs(Util.getTS(), rollPeriod));
        RollIdent roll = new RollIdent();
        roll.app = app;
        roll.period = rollPeriod;
        roll.source = remoteAddress;
        roll.ts = time.getTime();
        return roll;
    }

    private File getRollFile(RollIdent rollIdent) {
        String filename = rollIdent.app + '.' + rollIdent.source + '.' + format.format(rollIdent.ts) + node.getSuffix();
        return new File(storagedir, filename);
    }
    
    private void emit(String line) {
        // TODO send to realtime data comsumer
    }
}

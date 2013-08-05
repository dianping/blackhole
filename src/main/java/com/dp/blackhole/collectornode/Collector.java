package com.dp.blackhole.collectornode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
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
    
    public Collector(Collectornode server, Socket s, String home, String appname, long period) throws IOException {
        node = server;
        socket = s;
        remoteAddress = ((InetSocketAddress)socket.getRemoteSocketAddress()).getHostName();
        app = appname;
        rollPeriod = period;
        format = new SimpleDateFormat(Util.getFormatFromPeroid(period));
        storagedir = home+"/"+ app + "/" + remoteAddress;
        
        init();
    }
    
    public void init() throws IOException, IOException {
        appending = node.getappendingFile(storagedir);
        writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(appending)));
        streamIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }
    
    @Override
    public void run() {
        String event;
        try {
            while ((event = streamIn.readLine()) != null) {
                LOG.debug("received event: " + event);
                if (event.length() != 0) {
                    writetofile(event);
                    emit(event);
                } else {
                    completefile();
                }
            }
        } catch (IOException e) {
            handleIOException();
        }
    }
    
    private void handleIOException() {
        close();
    }
    
    private void close() {
        try {
        streamIn.close();
        socket.close();
        writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        appending.renameTo(new File(appending.toString()+ "." +  (new Date()).toString()));
    }

    private void writetofile(String line) throws IOException {
        writer.write(line);
        writer.write('\n');
    }

    private void completefile() throws IOException {
        writer.close();
        
        RollIdent rollIdent = getRollIdent();
        File rollFile = getRollFile(rollIdent);
        
        LOG.info("complete file: " + rollFile + ", roll " + rollIdent);
        
        appending.renameTo(rollFile);
        node.registerfile(rollIdent, rollFile);
        
        appending = node.getappendingFile(storagedir);
        writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(appending)));
    }

    private RollIdent getRollIdent() {
        Date time = new Date(Util.getRollTs(rollPeriod));
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

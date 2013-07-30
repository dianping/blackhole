package com.dp.blackhole.collectornode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

import com.dp.blackhole.common.Util;

public class Collector implements Runnable {

    String identifier;
    Socket socket;
    final String remoteAddress;
    final String app;
    BufferedWriter writer;
    BufferedReader streamIn;
    Collectornode node;
    String storagedir;
    File appending;
    long rollPeriod;
    SimpleDateFormat format;
    
    public Collector(String ident, Collectornode server, Socket s, String home, String appname, long Period, String rollFormat) {
        identifier = ident;
        node = server;
        socket = s;
        remoteAddress = socket.getRemoteSocketAddress().toString();
        app = appname;
        rollPeriod = Period;
        format = new SimpleDateFormat(rollFormat);
        storagedir = home+"/"+ app + "/" + remoteAddress;
    }
    
    public void init() throws IOException, IOException {
        appending = node.getappendingFile(storagedir);
        writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(appending))));
        streamIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));
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
        writer.newLine();
    }

    private void completefile() throws IOException {
        writer.close();
        
        RollIdent rollIdent = getRollIdent();
        File rollFile = getRollFile(rollIdent);
        appending.renameTo(rollFile);
        node.registerfile(rollIdent, rollFile);
        
        appending = node.getappendingFile(storagedir);
        writer = new BufferedWriter(new FileWriter(appending));
    }

    private RollIdent getRollIdent() {
        Date time = new Date(Util.getRollTs(rollPeriod));
        RollIdent roll = new RollIdent();
        roll.app = app;
        roll.source = remoteAddress;
        roll.ts = time.getTime();
        return roll;
    }

    private File getRollFile(RollIdent rollIdent) {
        String filename = rollIdent.app + rollIdent.source + format.format(rollIdent.ts);
        return new File(storagedir, filename);
    }
    
    private void emit(String line) {
        // TODO send to realtime data comsumer
    }

    public static void main(String args[]) throws IOException, IOException {
        
//        ServerSocket sc = new ServerSocket(8081);
//        Socket client = sc.accept();
//        Collector c = new Collector(client);
//        c.run();
//        System.out.println("done");

    }
}

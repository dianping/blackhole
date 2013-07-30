package com.dp.blackhole.collectornode;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dp.blackhole.common.AppRollPB.AppRoll;
import com.dp.blackhole.common.CollectorRegPB.CollectorReg;
import com.dp.blackhole.common.MessagePB.Message;
import com.dp.blackhole.common.MessagePB.Message.MessageType;
import com.dp.blackhole.common.ReadyCollectorPB.ReadyCollector;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.node.Node;

public class Collectornode extends Node {

    private ExecutorService pool;
    private ServerSocket server;
    private HashMap<String, Collector> collectors;
    private ConcurrentHashMap<RollIdent, File> fileRolls;
    private String basedir;
    private int port;

    public Collectornode() throws IOException {
        pool = Executors.newCachedThreadPool();
        collectors = new HashMap<String, Collector>();
        fileRolls = new ConcurrentHashMap<RollIdent, File>();
    }

    private class Acceptor extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    Socket socket = server.accept();
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    
                    String type = Util.readString(in);
                    String app = Util.readString(in);
                    long peroid = in.readLong(); 
                    String format = Util.readString(in);
                    if ("stream".equals(type)) {
                        StringBuffer buffer = new StringBuffer();
                        buffer.append("collector.")
                            .append(socket.getLocalAddress())
                            .append(".")
                            .append(app)
                            .append(".")
                            .append(Util.getTS());
                        String id = buffer.toString();
                        Collector collector = new Collector(id, Collectornode.this, socket, basedir, app, peroid, format);
                        pool.execute(collector);
                        collectors.put(id, collector);
                        send(getConnectedMsg(app, socket.getRemoteSocketAddress().toString(), socket.getLocalSocketAddress().toString(), Util.getTS()));;
                    } else if ("recovery".equals("type")) {
                        
                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
    
    public Message getConnectedMsg(String app, String appnode, String collectornode, long connectedTs) {
        ReadyCollector.Builder ReadyMessage = ReadyCollector.newBuilder();
        ReadyMessage.setAppName(app)
            .setAppServer(appnode)
            .setCollectorServer(collectornode)
            .setConnectedTs(connectedTs);
        Message.Builder message = Message.newBuilder();
        message.setType(MessageType.READY_COLLECTOR)
            .setReadyCollector(ReadyMessage.build());
        return message.build();
    }
    
    @Override
    protected void process(Message msg) {
        switch (msg.getType()) {
        case APP_REG:    
        default:
        }
    }

    private void start() throws FileNotFoundException, IOException {
        
        Properties prop = new Properties();
        prop.load(new FileReader(new File("config.properties")));
        
        basedir = prop.getProperty("collectornode.basedir");
        port = Integer.parseInt(prop.getProperty("collectornode.port"));
        
        server = new ServerSocket(port);
        
        cleanupLocalStorage();
        
        // start to accept connection
        Acceptor acceptor = new Acceptor();
        acceptor.setDaemon(true);
        acceptor.start();
        
        registerNode();
        
        loop();
        close();
    }
    
    private void delete(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            for (File f : children) {
                f.delete();
            }
            file.delete();
        } else {
            file.delete();
        }
    }
    
    private void cleanupLocalStorage() {
        File base = new File(basedir);
        if (base.exists()) {
            delete(base);
        }
        base.mkdir();
    }

    private void registerNode() {  
        CollectorReg.Builder collectorReg = CollectorReg.newBuilder();
        collectorReg.setServername(server.getLocalSocketAddress().toString())
            .setRegTs(Util.getTS());
        Message.Builder message = Message.newBuilder();
        message.setType(MessageType.COLLECTOR_REG);
        send(message.build());
        LOG.info("register collector node with supervisor");
    }

    private void close() {
        LOG.info("shutdown collector node");
        pool.shutdownNow();
        try {
            server.close();
        } catch (IOException e) {
            LOG.error("error close ServerSocket" + e);
        }
    }

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        try {
        Collectornode node = new Collectornode();
        node.start();
        } catch (Throwable e) {
            LOG.error("fatal eroor" + e);
            System.exit(-1);
        }
    }

    public File getappendingFile(String storagedir) {
        File parent = new File(storagedir);
        parent.mkdirs();
        File file = new File(parent, "appending." + Util.getTS() + ".gz");
        return file;
    }

    public void registerfile(RollIdent rollIdent, File rollFile) {
        if (fileRolls.get(rollIdent) == null) {
            fileRolls.put(rollIdent, rollFile);
            AppRoll.Builder appRoll = AppRoll.newBuilder();
            appRoll.setAppName(rollIdent.app)
                .setAppServer(rollIdent.source)
                .setRollTs(rollIdent.ts);
            Message.Builder message = Message.newBuilder();
            message.setType(MessageType.APP_ROLL);
            send(message.build());
        } else {
            LOG.fatal("update a exists file roll");
        }
    }
    
    public void recoveryResult(HDFSRecovery hdfsRecovery,
            boolean recoverySuccess) {
        // TODO Auto-generated method stub
        
    }

    public void uploadResult(HDFSUpload hdfsUpload, boolean uploadSuccess) {
        // TODO Auto-generated method stub
        
    }

}

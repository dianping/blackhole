package com.dp.blackhole.appnode;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.AgentProtocol;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.AgentProtocol.AgentHead;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.conf.ConfigKeeper;

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
            DataOutputStream out = new DataOutputStream(server.getOutputStream());

            AgentProtocol protocol = new AgentProtocol();
            AgentHead head = protocol.new AgentHead();
            
            head.type = AgentProtocol.STREAM;
            head.app = appLog.getAppName();
            head.peroid = ConfigKeeper.configMap.get(appLog.getAppName()).getLong(ParamsKey.Appconf.ROLL_PERIOD);
  
            protocol.sendHead(out, head);
            
            writer = new OutputStreamWriter(out);
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
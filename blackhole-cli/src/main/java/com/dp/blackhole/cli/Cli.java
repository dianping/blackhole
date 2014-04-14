package com.dp.blackhole.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.SimpleConnection;
import com.google.protobuf.InvalidProtocolBufferException;
import com.dp.blackhole.protocol.control.DumpReplyPB.DumpReply;
import com.dp.blackhole.protocol.control.MessagePB.Message;

public class Cli {
    private static final Log LOG = LogFactory.getLog(Cli.class);
    private String autoCmd;
    private CliProcessor processor;
    private SimpleConnection supervisor;
    private GenClient<ByteBuffer, SimpleConnection, CliProcessor> client;

    class CliInnerProcessor extends Thread {
        BufferedReader in;
        PrintStream out;
        PrintStream err;
        
        public CliInnerProcessor() {
            in = new BufferedReader(new InputStreamReader(System.in));
            out = new PrintStream(System.out);
            err = new PrintStream(System.err);
        }
        
        private String[] getTokens(String cmd) {
            return cmd.split("\\s+");
        }
        
        public void processCommand(String cmd) {
            try {
                if (cmd.equals("dumpstat")) {
                    Message msg = PBwrap.wrapDumpStat();
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.startsWith("dumpapp")) {
                    String[] tokens = getTokens(cmd);
                    String appName = tokens[1];
                    Message msg = PBwrap.wrapDumpApp(appName);
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.startsWith("recovery")) {
                    String[] tokens = getTokens(cmd);
                    String appName = tokens[1];
                    String appServer = tokens[2];
                    long rollTs = Long.parseLong(tokens[3]); 
                    Message msg = PBwrap.wrapManualRecoveryRoll(appName, appServer, rollTs);
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.startsWith("range")) {
                    //recovery -a 3600 1385276400000 1385301600000
                    String[] tokens = getTokens(cmd);
                    String appName = tokens[1];
                    String appServer = tokens[2];
                    long period = Long.parseLong(tokens[3]);
                    long startRollTs = Long.parseLong(tokens[4]);
                    long endRollTs = Long.parseLong(tokens[5]);
                    long recoveryStageCount = (endRollTs - startRollTs) / period / 1000;
                    for (int i = 0; i<= recoveryStageCount; i++) {
                        long rollTs = startRollTs + period * 1000 * (i);
                        Message msg = PBwrap.wrapManualRecoveryRoll(appName, appServer, rollTs);
                        send(msg);
                        out.println("send message: " + msg);
                    }
                } else if (cmd.startsWith("retire")) {
                    String[] tokens = getTokens(cmd);
                    String appName = tokens[1];
                    String appServer = tokens[2];
                    Message msg = PBwrap.wrapRetireStream(appName, appServer);
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.equals("dumpconf")) {
                    Message msg = PBwrap.wrapDumpConf();
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.equals("listapps")) {
                    Message msg = PBwrap.wrapListApps();
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.startsWith("rmconf")) {
                    String[] tokens = getTokens(cmd);
                    String appName = tokens[1];
                    ArrayList<String> appServers = new ArrayList<String>();
                    for (int i = 2; i < tokens.length; i++) {
                        appServers.add(tokens[i]);
                    }
                    Message msg = PBwrap.wrapRemoveConf(appName, appServers);
                    send(msg);
                } else if (cmd.equals("listidle")) {
                    Message msg = PBwrap.wrapListIdle();
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.equals("help")) {
                    out.println("Usage:");
                    out.println(" dumpstat              Display all of streams information.");
                    out.println(" dumpconf              Display all of app config information.");
                    out.println(" listapps              List all of application names.");
                    out.println(" listidle              List all of idle hostname or ip.");
                    out.println(" dumpapp <appname>     Display the stream, stages of the application followed.");
                    out.println(" rmconf <appname>      Remove the configuration specified by appname which should be useless.");
                    out.println("                       (Just remove from supervisor memory rather than lion/ZK).");
                    out.println(" retire <appname> <appserver>      Retire the stream specified by appname and appserver.");
                    out.println(" recovery <appname> <appserver> <rolltimestamp>");
                    out.println("                       Recovery the stream specified by appname, appserver and roll ts.");
                    out.println(" range <appname> <appserver> <period> <start rolltimestamp> <end rolltimestamp>");
                    out.println("                       Recovery a range of streams specified period from start timestamp to end timestamp.");
                    out.println(" quit                  Quit the command line interface.");
                    out.println(" help                  Display help information.");
                } else if (cmd.equals("quit")) {
                    System.exit(0);
                } else {
                    out.println("unknown command. Enter \"help\" for more information.");
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                out.println("Incorrect command format. Enter \"help\" for more information.");
            }
        }
        
        @Override
        public void run() {
            String cmd;
            
            out.println("blackhole cli started:");
            out.print("blackhole.cli>");
            
            try {
                while ((cmd = in.readLine()) != null) {
                    processCommand(cmd.trim());
                    out.print("blackhole.cli>");
                }
            } catch (Exception e) {
                e.printStackTrace(err);
            }
        }
    }
    
    class CloseTimer extends Thread {
        @Override
        public void run() {
            LOG.info("Currently Cli JVM will be terminated after 10 sec.");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOG.warn(e.getMessage());
            }
            LOG.info("Cli shutdown.");
            System.exit(0);
        }
    }
    
    public class CliProcessor implements EntityProcessor<ByteBuffer, SimpleConnection> {

        @Override
        public void OnConnected(SimpleConnection connection) {
            supervisor = connection;
            if (autoCmd == null) {
                return;
            }
            CliInnerProcessor processor = new CliInnerProcessor();
            processor.processCommand(autoCmd.trim());
            new CloseTimer().start();
        }

        @Override
        public void OnDisconnected(SimpleConnection connection) {
            supervisor.close();
            supervisor = null;
        }

        @Override
        public void process(ByteBuffer buf, SimpleConnection from) {
            Message message = null;;
            try {
                message = PBwrap.Buf2PB(buf);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("InvalidProtocolBufferException catched: ", e);
                return;
            }
            
            LOG.debug("received: " + message);
            
            switch (message.getType()) {
            case DUMPREPLY:
                DumpReply dumpReply = message.getDumpReply();
                PrintStream out = new PrintStream(System.out);
                out.println("receive reply: ");
                out.println(dumpReply.getReply());
                out.print("blackhole.cli>");
                break;
            default:
                LOG.error("Illegal message type " + message.getType());
            }
        }
        
    }
    
    public void send(Message msg) {
        if (supervisor != null) {
            supervisor.send(PBwrap.PB2Buf(msg));
        }
    }

    private void start() throws FileNotFoundException, IOException {
        Properties prop = new Properties();
        prop.load(new FileReader(new File("config.properties")));
        
        processor = new CliProcessor();
        client = new GenClient<ByteBuffer, SimpleConnection, Cli.CliProcessor>(
                processor,
                new SimpleConnection.SimpleConnectionFactory(),
                null);
        client.init(prop, "cli", "supervisor.host", "supervisor.port");
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException {
        Cli cli = new Cli();
        if (args.length > 0 && args[0].equals("auto")) {
            String cmd = new String();
            for (int i = 1; i < args.length; i++) {
                cmd += args[i] + " ";
            }
            LOG.info("auto command: " + cmd);
            cli.autoCmd = cmd.trim();
        } else {
            CliInnerProcessor processor = cli.new CliInnerProcessor();
            processor.setDaemon(true);
            processor.start();
        }
        cli.start();
    }
}

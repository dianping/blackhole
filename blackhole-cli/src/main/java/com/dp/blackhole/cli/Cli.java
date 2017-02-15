package com.dp.blackhole.cli;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.HeartBeat;
import com.dp.blackhole.network.NioService;
import com.dp.blackhole.network.ByteBufferNonblockingConnection;
import com.google.protobuf.InvalidProtocolBufferException;
import com.dp.blackhole.protocol.control.DumpReplyPB.DumpReply;
import com.dp.blackhole.protocol.control.MessagePB.Message;

public class Cli {
    private static final Log LOG = LogFactory.getLog(Cli.class);
    private String autoCmd;
    private CliProcessor processor;
    private ByteBufferNonblockingConnection supervisor;
    private GenClient<ByteBuffer, ByteBufferNonblockingConnection, CliProcessor> client;
    private static long terminatePeriod;

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
                } else if (cmd.startsWith("dumptopic")) {
                    String[] tokens = getTokens(cmd);
                    String topic = tokens[1];
                    Message msg = PBwrap.wrapDumpApp(topic);
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.startsWith("recovery")) {
                    String[] tokens = getTokens(cmd);
                    String topic = tokens[1];
                    String source = tokens[2];
                    long period = Long.parseLong(tokens[3]);
                    long rollTs = Long.parseLong(tokens[4]); 
                    Message msg = PBwrap.wrapManualRecoveryRoll(topic, source, period, rollTs);
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.startsWith("range")) {
                    //recovery -a 3600 1385276400000 1385301600000
                    String[] tokens = getTokens(cmd);
                    String topic = tokens[1];
                    String source = tokens[2];
                    long period = Long.parseLong(tokens[3]);
                    long startRollTs = Long.parseLong(tokens[4]);
                    long endRollTs = Long.parseLong(tokens[5]);
                    long recoveryStageCount = (endRollTs - startRollTs) / period / 1000;
                    for (int i = 0; i<= recoveryStageCount; i++) {
                        long rollTs = startRollTs + period * 1000 * (i);
                        Message msg = PBwrap.wrapManualRecoveryRoll(topic, source, period, rollTs);
                        send(msg);
                        out.println("send message: " + msg);
                    }
                } else if (cmd.startsWith("retire")) {
                    String[] tokens = getTokens(cmd);
                    String topic = tokens[1];
                    String source = tokens[2];
                    boolean force = false;
                    if (tokens.length > 3 && tokens[3].equalsIgnoreCase("--force")) {
                        force = true;
                    }
                    Message msg = PBwrap.wrapRetireStream(topic, source, force);
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.equals("dumpconf")) {
                    Message msg = PBwrap.wrapDumpConf();
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.equals("listtopic")) {
                    Message msg = PBwrap.wrapListApps();
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.startsWith("rmconf")) {
                    String[] tokens = getTokens(cmd);
                    String topic = tokens[1];
                    ArrayList<String> agentServers = new ArrayList<String>();
                    for (int i = 2; i < tokens.length; i++) {
                        agentServers.add(tokens[i]);
                    }
                    Message msg = PBwrap.wrapRemoveConf(topic, agentServers);
                    send(msg);
                } else if (cmd.equals("listidle")) {
                    Message msg = PBwrap.wrapListIdle();
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.startsWith("restart")) {
                    String[] tokens = getTokens(cmd);
                    ArrayList<String> agentServers = new ArrayList<String>();
                    for (int i = 1; i < tokens.length; i++) {
                        agentServers.add(tokens[i]);
                    }
                    Message msg = PBwrap.wrapRestart(agentServers);
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.startsWith("dumpcons")) {
                    String[] tokens = getTokens(cmd);
                    String topic;
                    String groupId;
                    if (tokens.length > 2) {
                        topic = tokens[1];
                        groupId = tokens[2];
                    } else {
                        String[] newTokens = tokens[1].split("/");
                        if (newTokens.length == 2) {
                            topic = newTokens[0];
                            groupId = newTokens[1];
                        } else {
                            throw new ArrayIndexOutOfBoundsException();
                        }
                    }
                    Message msg = PBwrap.wrapDumpConsumeGroup(topic, groupId);
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.equals("listcons")) {
                    Message msg = PBwrap.wrapListConsumerGroups();
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.equals("rmgroup")) {
                    String[] tokens = getTokens(cmd);
                    String topic = tokens[1];
                    String groupId = tokens[2];
                    Message msg = PBwrap.wrapRetireConsumerGroup(topic, groupId);
                    send(msg);
                    out.println("send message: " + msg);
                } else if (cmd.equals("help")) {
                    out.println("Usage:");
                    out.println(" dumpstat              Display all of streams information.");
                    out.println(" dumpconf              Display all of topic config information.");
                    out.println(" listtopic             List all of topic names.");
                    out.println(" listidle              List all of idle hostname or ip.");
                    out.println(" dumptopic <topic>     Display the stream, stages of the topic followed.");
                    out.println(" rmconf <topic>      Remove the configuration specified by topic which should be useless.");
                    out.println("                       (Just remove from supervisor memory rather than lion/ZK).");
                    out.println(" retire <topic> <agentserver>      Retire the stream specified by topic and agentServer.");
                    out.println(" recovery <topic> <agentserver> <period> <rolltimestamp>");
                    out.println("                       Recovery the stream specified by topic, agentserver and roll ts.");
                    out.println(" range <topic> <agentserver> <period> <start rolltimestamp> <end rolltimestamp>");
                    out.println("                       Recovery a range of streams specified period from start timestamp to end timestamp.");
                    out.println(" restart [agentservers]  Restart a set of agents in one command. The agents are splitted by \" \"");
                    out.println(" dumpcons <topic>/<consumerGroupId>         Display the consumer group information.");
                    out.println(" listcons              List all of consumer group, contains topic and group name");
                    out.println(" rmgroup <topic> <groupId>         Retire the consumer group specified by topic and groupId");
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
            LOG.info("Currently Cli JVM will be terminated after " + terminatePeriod + " sec.");
            try {
                Thread.sleep(terminatePeriod);
            } catch (InterruptedException e) {
                LOG.warn(e.getMessage());
            }
            LOG.info("Cli shutdown.");
            System.exit(0);
        }
    }
    
    public class CliProcessor implements EntityProcessor<ByteBuffer, ByteBufferNonblockingConnection> {
        private HeartBeat heartbeat = null;
        @Override
        public void OnConnected(ByteBufferNonblockingConnection connection) {
            supervisor = connection;
            if (autoCmd == null) {
                heartbeat = new HeartBeat(supervisor);
                heartbeat.start();
                return;
            }
            CliInnerProcessor processor = new CliInnerProcessor();
            processor.processCommand(autoCmd.trim());
            new CloseTimer().start();
        }

        @Override
        public void OnDisconnected(ByteBufferNonblockingConnection connection) {
            supervisor = null;
        }

        @Override
        public void receiveTimout(ByteBuffer msg, ByteBufferNonblockingConnection conn) {

        }

        @Override
        public void sendFailure(ByteBuffer msg, ByteBufferNonblockingConnection conn) {

        }

        @Override
        public void setNioService(NioService<ByteBuffer, ByteBufferNonblockingConnection> service) {

        }

        @Override
        public void process(ByteBuffer buf, ByteBufferNonblockingConnection from) {
            Message message = null;;
            try {
                message = PBwrap.Buf2PB(buf);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("InvalidProtocolBufferException catched: ", e);
                return;
            }
            
            switch (message.getType()) {
            case DUMP_REPLY:
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
        Util.send(supervisor, msg);
    }

    private void start() throws FileNotFoundException, IOException {
        Properties prop = new Properties();
        prop.load(getClass().getClassLoader().getResourceAsStream("config.properties"));
        terminatePeriod = Long.parseLong(prop.getProperty("cli.terminate.period", "1000"));
        String supervisorHost = prop.getProperty("supervisor.host");
        int supervisorPort = Integer.parseInt(prop.getProperty("supervisor.port"));
        processor = new CliProcessor();
        client = new GenClient<ByteBuffer, ByteBufferNonblockingConnection, Cli.CliProcessor>(
                processor,
                new ByteBufferNonblockingConnection.ByteBufferNonblockingConnectionFactory(),
                null);
        client.init("cli", supervisorHost, supervisorPort);
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

package com.dp.blackhole.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Properties;

import org.apache.tools.ant.filters.TokenFilter.StringTokenizer;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.node.Node;

public class Cli extends Node {

    class CliProcessor extends Thread {
        BufferedReader in;
        PrintStream out;
        PrintStream err;
        StringTokenizer tokenizer;
        
        public CliProcessor() {
            in = new BufferedReader(new InputStreamReader(System.in));
            out = new PrintStream(System.out);
            err = new PrintStream(System.err);
        }
        
        private String[] getTokens(String cmd) {
            return cmd.split("\\s+");
        }
        
        private void processCommand(String cmd) {
            if (cmd.equals("dumpstat")) {
                Message msg = PBwrap.wrapDumpStat();
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
            } else if (cmd.startsWith("retire")) {
                String[] tokens = getTokens(cmd);
                String appName = tokens[1];
                String appServer = tokens[2];
                Message msg = PBwrap.wrapRetireStream(appName, appServer);
                send(msg);
                out.println("send message: " + msg);
            } else if (cmd.equals("quit")) {
                System.exit(0);
            } else {
                out.println("unknown command");
            }
        }
        
        @Override
        public void run() {
            String cmd;
            
            out.println("blackhole cli started:");
            out.print("blackhole.cli>");
            
            try {
                while ((cmd = in.readLine()) != null) {
                    processCommand(cmd);
                    out.print("blackhole.cli>");
                }
            } catch (Exception e) {
                e.printStackTrace(err);
            }
        }
    }
    
    @Override
    protected boolean process(Message msg) {
        return false;
    }

    private void start() throws FileNotFoundException, IOException {
        Properties prop = new Properties();
        prop.load(new FileReader(new File("config.properties")));
        
        String serverhost = prop.getProperty("supervisor.host");
        int serverport = Integer.parseInt(prop.getProperty("supervisor.port"));
        
        init(serverhost, serverport);
        
        CliProcessor processor = new CliProcessor();
        processor.setDaemon(true);
        processor.start();
        
        loop();
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException {
        Cli cli = new Cli();
        cli.start();
    }
}

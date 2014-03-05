package com.dp.blackhole.collectornode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;

import com.dp.blackhole.collectornode.persistent.PersistentManager.reporter.ReportEntry;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.RollIDPB.RollID;
import com.dp.blackhole.network.SimpleConnection;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.google.protobuf.InvalidProtocolBufferException;

public class Broker {
    final static Log LOG = LogFactory.getLog(Broker.class);
    
    private static Broker broker;

    private static RollManager rollMgr;
    private BrokerProcessor processor;
    private GenClient<ByteBuffer, SimpleConnection, BrokerProcessor> client;

    private BrokerService brokerservice;
    
    public Broker() throws IOException {
        rollMgr = new RollManager();
        broker = this;
    }

    private void start() throws FileNotFoundException, IOException {
        Properties prop = new Properties();
        prop.load(new FileReader(new File("config.properties")));
        
        int recoveryPort = Integer.parseInt(prop.getProperty("broker.recovery.port"));
        String hdfsbasedir = prop.getProperty("broker.hdfs.basedir");
        String suffix = prop.getProperty("broker.hdfs.file.suffix");
        boolean enableSecurity = Boolean.parseBoolean(prop.getProperty("broker.hdfs.security.enable", "true"));
        
        if (enableSecurity) {
            String keytab = prop.getProperty("broker.blackhole.keytab");
            String principal = prop.getProperty("broker.blackhole.principal");
            Configuration conf = new Configuration();
            conf.set("broker.blackhole.keytab", keytab);
            conf.set("broker.blackhole.principal", principal);
            HDFSLogin(conf, "broker.blackhole.keytab", "broker.blackhole.principal");
        }
        
        rollMgr.init(hdfsbasedir, suffix, recoveryPort);
        
        brokerservice = new BrokerService(prop);
        brokerservice.setDaemon(true);
        brokerservice.start();
        
        // start GenClient
        processor = new BrokerProcessor();
        client = new GenClient(
                processor,
                new SimpleConnection.SimpleConnectionFactory(),
                new ControlMessageTypeFactory());
        client.init(prop, "broker", "supervisor.host", "supervisor.port");
        
        rollMgr.close();
    }
    
    private void registerNode() {
        send(PBwrap.wrapCollectorReg());
        LOG.info("register collector node with supervisor");
    }

    private void HDFSLogin(Configuration conf, String keytab, String principle) throws IOException {        
        SecurityUtil.login(conf, keytab, principle);
    }

    public void sendMsg(Message message) {
        send(message);
    }
    
    public void reportPartitionInfo(List<ReportEntry> entrylist) {
        send(PBwrap.wrapTopicReport(entrylist));
    }
    
    public void send(Message msg) {
        LOG.debug("send: " + msg);
        processor.send(PBwrap.PB2Buf(msg));
    }
    
    public static Broker getSupervisor() {
        return broker;
    }
    
    public static RollManager getRollMgr() {
        return rollMgr;
    }
    
    class BrokerProcessor implements EntityProcessor<ByteBuffer, SimpleConnection> {
        private SimpleConnection supervisor;
        
        @Override
        public void OnConnected(SimpleConnection connection) {
            supervisor = connection;          
            registerNode();
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
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            LOG.debug("received: " + message);
            
            switch (message.getType()) {
            case UPLOAD_ROLL:
                RollID rollID = null;;
                rollID = message.getRollID();
                rollMgr.doUpload(rollID);
                break;
            default:
                LOG.error("response type is undefined");
                break;
            }
            
        }
        
        void send(ByteBuffer wrap) {
            if (supervisor != null) {
                supervisor.send(wrap);
            }
        }
    }
    
    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
    try {
        Broker broker = new Broker();
        broker.start();
    } catch (Exception e) {
            LOG.error("fatal error ", e);
            System.exit(-1);
        }
    }
}
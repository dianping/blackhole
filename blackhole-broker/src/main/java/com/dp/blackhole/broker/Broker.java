package com.dp.blackhole.broker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;

import com.dp.blackhole.broker.ftp.FTPConfigrationLoader;
import com.dp.blackhole.broker.storage.StorageManager.reporter.ReportEntry;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.HeartBeat;
import com.dp.blackhole.network.SimpleConnection;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.MessagePB.Message.MessageType;
import com.dp.blackhole.protocol.control.RollIDPB.RollID;
import com.dp.blackhole.protocol.control.TopicReportPB.TopicReport;
import com.google.protobuf.InvalidProtocolBufferException;

public class Broker {
    final static Log LOG = LogFactory.getLog(Broker.class);
    
    private static Broker broker;
    private static BrokerService brokerService;
    private static RollManager rollMgr;
    private BrokerProcessor processor;
    private SimpleConnection supervisor;
    private GenClient<ByteBuffer, SimpleConnection, BrokerProcessor> client;
    private int servicePort;
    private int recoveryPort;
    
    public Broker() throws IOException {
        rollMgr = new RollManager();
        broker = this;
    }

    private void start() throws FileNotFoundException, IOException {
        Properties prop = new Properties();
        prop.load(getClass().getClassLoader().getResourceAsStream("config.properties"));
        String supervisorHost = prop.getProperty("supervisor.host");
        int supervisorPort = Integer.parseInt(prop.getProperty("supervisor.port"));
        servicePort =  Integer.parseInt(prop.getProperty("broker.service.port"));
        recoveryPort = Integer.parseInt(prop.getProperty("broker.recovery.port"));
        String hdfsbasedir = prop.getProperty("broker.hdfs.basedir");
        String suffix = prop.getProperty("broker.hdfs.file.suffix");
        long clockSyncBufMillis = Long.parseLong(prop.getProperty("broker.rollmanager.clockSyncBufMillis", "5000"));
        int maxUploadThreads = Integer.parseInt(prop.getProperty("broker.rollmanager.maxUploadThreads", "20"));
        int maxRecoveryThreads = Integer.parseInt(prop.getProperty("broker.rollmanager.maxRecoveryThreads", "10"));
        int recoverySocketTimeout = Integer.parseInt(prop.getProperty("broker.rollmanager.recoverySocketTimeout", "600000"));

        boolean enableSecurity = Boolean.parseBoolean(prop.getProperty("broker.hdfs.security.enable", "true"));
        if (enableSecurity) {
            String keytab = prop.getProperty("broker.blackhole.keytab");
            String principal = prop.getProperty("broker.blackhole.principal");
            Configuration conf = new Configuration();
            conf.set("broker.blackhole.keytab", keytab);
            conf.set("broker.blackhole.principal", principal);
            HDFSLogin(conf, "broker.blackhole.keytab", "broker.blackhole.principal");
        }
        
        boolean enableFTP = Boolean.parseBoolean(prop.getProperty("broker.storage.ftp.enable", "false"));
        if (enableFTP) {
            long ftpConfigrationCheckInterval = Long.parseLong(prop.getProperty("broker.storage.ftp.configCheckIntervalMilli", "600000"));
            FTPConfigrationLoader ftpConfigrationLoader =  new FTPConfigrationLoader(ftpConfigrationCheckInterval);
            Thread thread = new Thread(ftpConfigrationLoader);
            thread.start();
        }
        
        rollMgr.init(hdfsbasedir, suffix, recoveryPort, clockSyncBufMillis, maxUploadThreads, maxRecoveryThreads, recoverySocketTimeout);
        
        brokerService = new BrokerService(prop);
        brokerService.setDaemon(true);
        brokerService.start();
        
        // start GenClient
        processor = new BrokerProcessor();
        client = new GenClient(
                processor,
                new SimpleConnection.SimpleConnectionFactory(),
                null);
        client.init("broker", supervisorHost, supervisorPort);
        
        rollMgr.close();
    }
    
    private void registerNode() {
        send(PBwrap.wrapBrokerReg(servicePort, recoveryPort));
        LOG.info("register broker node with supervisor");
    }

    private void HDFSLogin(Configuration conf, String keytab, String principle) throws IOException {        
        SecurityUtil.login(conf, keytab, principle);
    }
    
    public void reportPartitionInfo(List<ReportEntry> entrylist) {
        List<TopicReport.TopicEntry> topicEntryList = new ArrayList<TopicReport.TopicEntry>(entrylist.size());
        for (ReportEntry entry : entrylist) {
            TopicReport.TopicEntry topicEntry = PBwrap.getTopicEntry(entry.topic, entry.partition, entry.offset);
            topicEntryList.add(topicEntry);
        }
        send(PBwrap.wrapTopicReport(topicEntryList));
    }
    
    public void send(Message msg) {
        if (msg.getType() != MessageType.TOPICREPORT) {
            LOG.debug("send: " + msg);
        }
        Util.send(supervisor, msg);
    }
    
    public static Broker getSupervisor() {
        return broker;
    }
    
    public static RollManager getRollMgr() {
        return rollMgr;
    }
    
    public static BrokerService getBrokerService() {
        return brokerService;
    }
    
    class BrokerProcessor implements EntityProcessor<ByteBuffer, SimpleConnection> {
        private HeartBeat heartbeat = null;
        
        @Override
        public void OnConnected(SimpleConnection connection) {
            supervisor = connection;          
            registerNode();
            heartbeat = new HeartBeat(supervisor);
            heartbeat.start();
        }

        @Override
        public void OnDisconnected(SimpleConnection connection) {
            supervisor = null;
            brokerService.disconnectClients();
            heartbeat.shutdown();
            heartbeat = null;
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
            RollID rollID = null;
            switch (message.getType()) {
            case UPLOAD_ROLL:
                rollID = message.getRollID();
                rollMgr.doUpload(rollID);
                break;
            case MAKR_UNRECOVERABLE:
                rollID = message.getRollID();
                rollMgr.markUnrecoverable(rollID);
                break;
            default:
                LOG.error("response type is undefined");
                break;
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
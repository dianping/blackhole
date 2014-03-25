package com.dp.blackhole.collectornode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.collectornode.persistent.FileMessageSet;
import com.dp.blackhole.collectornode.persistent.MessageAndOffset;
import com.dp.blackhole.collectornode.persistent.MessageSet;
import com.dp.blackhole.collectornode.persistent.Partition;
import com.dp.blackhole.collectornode.persistent.PersistentManager;
import com.dp.blackhole.collectornode.persistent.PersistentManager.reporter.ReportEntry;
import com.dp.blackhole.collectornode.persistent.RollPartition;
import com.dp.blackhole.collectornode.persistent.protocol.FetchReply;
import com.dp.blackhole.collectornode.persistent.protocol.FetchRequest;
import com.dp.blackhole.collectornode.persistent.protocol.DataMessageTypeFactory;
import com.dp.blackhole.collectornode.persistent.protocol.MultiFetchReply;
import com.dp.blackhole.collectornode.persistent.protocol.MultiFetchRequest;
import com.dp.blackhole.collectornode.persistent.protocol.OffsetReply;
import com.dp.blackhole.collectornode.persistent.protocol.OffsetRequest;
import com.dp.blackhole.collectornode.persistent.protocol.ProduceRequest;
import com.dp.blackhole.collectornode.persistent.protocol.RegisterRequest;
import com.dp.blackhole.collectornode.persistent.protocol.RotateRequest;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.network.ConnectionFactory;
import com.dp.blackhole.network.DelegationIOConnection;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenServer;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.network.TypedFactory;

public class BrokerService extends Thread {
    private final Log LOG = LogFactory.getLog(BrokerService.class);
    
    GenServer<TransferWrap, DelegationIOConnection, EntityProcessor<TransferWrap, DelegationIOConnection>> server;
    PersistentManager manager;
    PublisherExecutor executor;
    private Map<DelegationIOConnection, ClientDesc> clients;
    Properties prop;
    
    public static void reportPartitionInfo(List<ReportEntry> entrylist) {
        Broker.getSupervisor().reportPartitionInfo(entrylist);
    }
    
    @Override
    public void run() {
        try {
            server.init(prop, "Publisher", "broker.service.port");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public BrokerService(Properties prop) throws IOException {
        this.prop = prop;
        String storagedir = prop.getProperty("broker.storage.dir");
        int splitThreshold = Integer.parseInt(prop.getProperty("publisher.storage.splitThreshold", "536870912"));
        int flushThreshold = Integer.parseInt(prop.getProperty("publisher.storage.flushThreshold", "4194304"));
        clients = Collections.synchronizedMap(new HashMap<DelegationIOConnection, ClientDesc>());
        manager = new PersistentManager(storagedir, splitThreshold, flushThreshold);
        executor = new PublisherExecutor();
        ConnectionFactory<DelegationIOConnection> factory = new DelegationIOConnection.DelegationIOConnectionFactory();
        TypedFactory wrappedFactory = new DataMessageTypeFactory();
        server = new GenServer<TransferWrap, DelegationIOConnection, EntityProcessor<TransferWrap, DelegationIOConnection>>
            (executor, factory, wrappedFactory);
    }
    
    public PublisherExecutor getExecutor() {
        return executor;
    }
    
    public PersistentManager getPersistentManager() {
        return manager;
    }
    
    public void disconnectClients() {
        for (DelegationIOConnection client : clients.keySet()) {
            client.close();
        }
        clients.clear();
    }
    
    public class PublisherExecutor implements
            EntityProcessor<TransferWrap, DelegationIOConnection> {

        public void handleProduceRequest(ProduceRequest request,
                DelegationIOConnection from) {
            try {
                Partition p = manager.getPartition(request.topic,
                        request.partitionId);
                p.append(request.getMesssageSet());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        public void handleFetchRequest(FetchRequest request,
                DelegationIOConnection from) {
            Partition p = null;
            FileMessageSet messages = null;
            try {
                p = manager.getPartition(request.topic, request.partitionId);
                messages = p.read(request.offset, request.limit);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            TransferWrap reply = null;
            if (messages == null) {
                reply = new TransferWrap(new FetchReply(p.getId(), messages, MessageAndOffset.OFFSET_OUT_OF_RANGE));
            } else {
                reply = new TransferWrap(new FetchReply(p.getId(), messages, request.offset));
            }
            from.send(reply);
        }

        public void handleMultiFetchRequest(MultiFetchRequest request,
                DelegationIOConnection from) {
            ArrayList<String> partitionList = new ArrayList<String>();
            ArrayList<MessageSet> messagesList = new ArrayList<MessageSet>();
            ArrayList<Long> offsetList = new ArrayList<Long>();
            for (FetchRequest f : request.fetches) {
                Partition p = null;
                FileMessageSet messages = null;
                try {
                    p = manager.getPartition(f.topic, f.partitionId);
                    messages = p.read(f.offset, f.limit);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                partitionList.add(p.getId());
                messagesList.add(messages);
                offsetList.add(messages.getOffset());
            }

            from.send(new TransferWrap(new MultiFetchReply(partitionList,
                    messagesList, offsetList)));
        }

        public void handleOffsetRequest(OffsetRequest request,
                DelegationIOConnection from) {
            Partition p = null;
            try {
                p = manager.getPartition(request.topic, request.partition);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            from.send(new TransferWrap(new OffsetReply(request.topic, request.partition, p.getEndOffset())));
        }
        
        public void handleRotateRequest(RotateRequest request, DelegationIOConnection from) {
            Partition p = null;
            try {
                p = manager.getPartition(request.topic, request.partitionId);
                RollPartition roll = p.markRotate();
                Broker.getRollMgr().doRegister(request.topic, request.partitionId, request.rollPeriod, roll);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        public void handleRegisterRequest(RegisterRequest request, DelegationIOConnection from) {
            clients.put(from, new ClientDesc(request.topic, ClientDesc.AGENT));
            Message msg = PBwrap.wrapReadyCollector(request.topic, request.source, request.peroid, request.broker, Util.getTS());
            Broker.getSupervisor().send(msg);
        }
        
        @Override
        public void process(TransferWrap request, DelegationIOConnection from) {
            switch (request.getType()) {
            case DataMessageTypeFactory.MultiFetchRequest:
                handleMultiFetchRequest((MultiFetchRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.FetchRequest:
                handleFetchRequest((FetchRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.OffsetRequest:
                handleOffsetRequest((OffsetRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.produceRequest:
                handleProduceRequest((ProduceRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.RotateRequest:
                handleRotateRequest((RotateRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.RegisterRequest:
                handleRegisterRequest((RegisterRequest) request.unwrap(), from);
                break;
            default:
                LOG.error("unknown message type: " + request.getType());
            }
        }

        @Override
        public void OnConnected(DelegationIOConnection connection) {
            clients.put(connection, new ClientDesc(ClientDesc.CONSUMER));
        }

        @Override
        public void OnDisconnected(DelegationIOConnection connection) {
            ClientDesc desc = clients.get(connection);
            if (desc.type == ClientDesc.AGENT) {
                Broker.getRollMgr().reportFailure(desc.topic, connection.getHost(), Util.getTS());
            }
            clients.remove(connection);
        }

    }

    class ClientDesc {
        public static final int AGENT = 0;
        public static final int CONSUMER = 1;
        
        public String topic;
        public int type;
        
        public ClientDesc (String topic, int type) {
            this.topic = topic;
            this.type = type;
        }
        
        public ClientDesc(int type) {
            this.type = type;
        }
    }
    
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("GenServer.handlercount", "1");
        properties.setProperty("GenServer.port", "2222");
        properties.setProperty("publisher.storage.dir", "/tmp/base");
        BrokerService pubservice = new BrokerService(properties);
        pubservice.run();
    }
}

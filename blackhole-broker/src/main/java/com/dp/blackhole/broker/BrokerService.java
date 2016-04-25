package com.dp.blackhole.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.broker.storage.Partition;
import com.dp.blackhole.broker.storage.StorageManager;
import com.dp.blackhole.broker.storage.RollPartition;
import com.dp.blackhole.broker.storage.StorageManager.Reporter.ReportEntry;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.NonblockingConnectionFactory;
import com.dp.blackhole.network.TransferWrapNonblockingConnection;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenServer;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.network.TypedFactory;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.data.DataMessageTypeFactory;
import com.dp.blackhole.protocol.data.FetchReply;
import com.dp.blackhole.protocol.data.FetchRequest;
import com.dp.blackhole.protocol.data.HaltRequest;
import com.dp.blackhole.protocol.data.MultiFetchReply;
import com.dp.blackhole.protocol.data.MultiFetchRequest;
import com.dp.blackhole.protocol.data.OffsetReply;
import com.dp.blackhole.protocol.data.OffsetRequest;
import com.dp.blackhole.protocol.data.ProduceRequest;
import com.dp.blackhole.protocol.data.ProducerRegReply;
import com.dp.blackhole.protocol.data.RegisterRequest;
import com.dp.blackhole.protocol.data.RollRequest;
import com.dp.blackhole.storage.FileMessageSet;
import com.dp.blackhole.storage.MessageAndOffset;
import com.dp.blackhole.storage.MessageSet;

public class BrokerService extends Thread {
    private final Log LOG = LogFactory.getLog(BrokerService.class);
    
    GenServer<TransferWrap, TransferWrapNonblockingConnection, EntityProcessor<TransferWrap, TransferWrapNonblockingConnection>> server;
    StorageManager manager;
    PublisherExecutor executor;
    private Map<TransferWrapNonblockingConnection, ClientDesc> clients;
    int servicePort;
    int numHandler;
    String localhost;
    
    public static void reportPartitionInfo(List<ReportEntry> entrylist) {
        Broker.getSupervisor().reportPartitionInfo(entrylist);
    }
    
    @Override
    public void run() {
        try {
            server.init("Publisher", servicePort, numHandler);
        } catch (IOException e) {
            LOG.error("Failed to init GenServer", e);
        }
    }
    
    public BrokerService(Properties prop) throws IOException {
        this.setName("BrokerService");
        localhost = Util.getLocalHost();
        String storagedir = prop.getProperty("broker.storage.dir");
        int splitThreshold = Integer.parseInt(prop.getProperty("broker.storage.splitThreshold", "536870912"));
        int flushThreshold = Integer.parseInt(prop.getProperty("broker.storage.flushThreshold", "4194304"));
        numHandler = Integer.parseInt(prop.getProperty("GenServer.handler.count", "3"));
        servicePort = Integer.parseInt(prop.getProperty("broker.service.port"));
        clients = new ConcurrentHashMap<TransferWrapNonblockingConnection, BrokerService.ClientDesc>();
        manager = new StorageManager(storagedir, splitThreshold, flushThreshold);
        executor = new PublisherExecutor();
        NonblockingConnectionFactory<TransferWrapNonblockingConnection> factory = new TransferWrapNonblockingConnection.TransferWrapNonblockingConnectionFactory();
        TypedFactory wrappedFactory = new DataMessageTypeFactory();
        server = new GenServer<TransferWrap, TransferWrapNonblockingConnection, EntityProcessor<TransferWrap, TransferWrapNonblockingConnection>>
            (executor, factory, wrappedFactory);
    }
    
    public PublisherExecutor getExecutor() {
        return executor;
    }
    
    public StorageManager getPersistentManager() {
        return manager;
    }
    
    public void disconnectClients() {
        for (TransferWrapNonblockingConnection client : clients.keySet()) {
            server.closeConnection(client);
        }
    }
    
    public class PublisherExecutor implements
            EntityProcessor<TransferWrap, TransferWrapNonblockingConnection> {
        
        private void closeClientOfErrorRequest(TransferWrapNonblockingConnection from, Object request) {
            LOG.info("can't find topic/partition with " + request + " from " + from +" ,close connection");
            server.closeConnection(from);
        }

        public void handleRegisterRequest(RegisterRequest request, TransferWrapNonblockingConnection from) {
            clients.put(from, new ClientDesc(request.topic, ClientDesc.AGENT, request.partitionId));
            boolean success = false;
            try {
                success = manager.createPartition(request.topic, request.partitionId);
            } catch (IOException e) {
                LOG.error("Got an IOE", e);
            }
            if (success) {
                Message msg = PBwrap.wrapReadyStream(request.topic, request.partitionId, request.period, localhost, Util.getTS());
                Broker.getSupervisor().send(msg);
                manager.storageRollPeriod(request.topic, request.period);
            }
            ProducerRegReply reply = new ProducerRegReply(success);
            from.send(new TransferWrap(reply));
        }

        public void handleProduceRequest(ProduceRequest request,
                TransferWrapNonblockingConnection from) {
            Partition p = manager.getPartition(request.topic, request.partitionId);
            if (p != null) {
                try {
                    p.append(request.getMesssageSet());
                } catch (IOException e) {
                    LOG.error("IOE catched", e);
                }
            } else {
                Util.logError(LOG, null, "can not get partition", request.topic, request.partitionId);
            }
        }

        public void handleFetchRequest(FetchRequest request,
                TransferWrapNonblockingConnection from) {
            Partition p = manager.getPartition(request.topic, request.partitionId);
            if (p == null) {
                closeClientOfErrorRequest(from, request);
                return;
            }
            FileMessageSet messages = p.read(request.offset, request.limit);
            
            TransferWrap reply = null;
            if (messages == null) {
                reply = new TransferWrap(new FetchReply(p.getId(), messages, MessageAndOffset.OFFSET_OUT_OF_RANGE));
                LOG.warn("Found offset out of range for " + request);
            } else {
                reply = new TransferWrap(new FetchReply(p.getId(), messages, request.offset));
            }
            from.send(reply);
        }

        public void handleMultiFetchRequest(MultiFetchRequest request,
                TransferWrapNonblockingConnection from) {
            ArrayList<String> partitionList = new ArrayList<String>();
            ArrayList<MessageSet> messagesList = new ArrayList<MessageSet>();
            ArrayList<Long> offsetList = new ArrayList<Long>();
            for (FetchRequest f : request.fetches) {
                Partition p = manager.getPartition(f.topic, f.partitionId);
                if (p == null) {
                    closeClientOfErrorRequest(from, request);
                    return;
                }
                FileMessageSet messages = p.read(f.offset, f.limit);
                partitionList.add(p.getId());
                messagesList.add(messages);
                offsetList.add(messages.getOffset());
            }

            from.send(new TransferWrap(new MultiFetchReply(partitionList,
                    messagesList, offsetList)));
        }

        public void handleOffsetRequest(OffsetRequest request,
                TransferWrapNonblockingConnection from) {
            Partition p = manager.getPartition(request.topic, request.partition);
            if (p == null) {
                closeClientOfErrorRequest(from, request);
                return;
            }
            if (request.autoOffset == OffsetRequest.EARLIES_OFFSET) {
                from.send(new TransferWrap(new OffsetReply(request.topic, request.partition, p.getStartOffset())));
            } else {
                from.send(new TransferWrap(new OffsetReply(request.topic, request.partition, p.getEndOffset())));
            }
        }
        
        public void handleRollRequest(RollRequest request, TransferWrapNonblockingConnection from) {
            Partition p = manager.getPartition(request.topic, request.partitionId);
            if (p == null) {
                closeClientOfErrorRequest(from, request);
                return;
            }
            try {
                RollPartition roll = p.markRollPartition();
                Broker.getRollMgr().perpareUpload(request.topic, request.partitionId, request.rollPeriod, roll);
            } catch (IOException e) {
                LOG.error("Got an IOE", e);
            }
        }
        
        public void handleLastRotateRequest(HaltRequest request, TransferWrapNonblockingConnection from) {
            Partition p = manager.getPartition(request.topic, request.partitionId);
            if (p == null) {
                closeClientOfErrorRequest(from, request);
                return;
            }
            try {
                RollPartition roll = p.markRollPartition();
                Broker.getRollMgr().doClean(request.topic, request.partitionId, request.rollPeriod, roll);
            } catch (IOException e) {
                LOG.error("Got an IOE", e);
            }
        }
        
        @Override
        public void process(TransferWrap request, TransferWrapNonblockingConnection from) {
            switch (request.getType()) {
            case DataMessageTypeFactory.RegisterRequest:
                handleRegisterRequest((RegisterRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.ProduceRequest:
                handleProduceRequest((ProduceRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.RotateOrRollRequest:
                handleRollRequest((RollRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.LastRotateRequest:
                handleLastRotateRequest((HaltRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.MultiFetchRequest:
                handleMultiFetchRequest((MultiFetchRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.FetchRequest:
                handleFetchRequest((FetchRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.OffsetRequest:
                handleOffsetRequest((OffsetRequest) request.unwrap(), from);
                break;
            case DataMessageTypeFactory.Heartbeat:
                break;
            default:
                LOG.error("unknown message type: " + request.getType());
            }
        }

        @Override
        public void OnConnected(TransferWrapNonblockingConnection connection) {
            LOG.info(connection + " connected");
            clients.put(connection, new ClientDesc(ClientDesc.CONSUMER));
        }

        @Override
        public void OnDisconnected(TransferWrapNonblockingConnection connection) {
            LOG.info(connection + " disconnected");
            ClientDesc desc = clients.get(connection);
            if (desc.type == ClientDesc.AGENT) {
                manager.removePartition(desc.topic, desc.source);
                Broker.getRollMgr().reportFailure(desc.topic, desc.source, Util.getTS());
            }
            clients.remove(connection);
        }

    }

    class ClientDesc {
        public static final int AGENT = 0;
        public static final int CONSUMER = 1;
        
        public String topic;
        public int type;
        public String source;
        
        public ClientDesc (String topic, int type, String source) {
            this.topic = topic;
            this.type = type;
            this.source = source;
        }
        
        public ClientDesc(int type) {
            this.type = type;
        }
    }
    
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("GenServer.handlercount", "1");
        properties.setProperty("GenServer.port", "2222");
        properties.setProperty("broker.storage.dir", "/tmp/base");
        BrokerService pubservice = new BrokerService(properties);
        pubservice.run();
    }
}

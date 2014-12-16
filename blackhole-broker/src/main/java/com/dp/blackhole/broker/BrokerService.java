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
import com.dp.blackhole.broker.storage.StorageManager.reporter.ReportEntry;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.ConnectionFactory;
import com.dp.blackhole.network.DelegationIOConnection;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenServer;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.network.TypedFactory;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.data.DataMessageTypeFactory;
import com.dp.blackhole.protocol.data.FetchReply;
import com.dp.blackhole.protocol.data.FetchRequest;
import com.dp.blackhole.protocol.data.LastRotateRequest;
import com.dp.blackhole.protocol.data.MultiFetchReply;
import com.dp.blackhole.protocol.data.MultiFetchRequest;
import com.dp.blackhole.protocol.data.OffsetReply;
import com.dp.blackhole.protocol.data.OffsetRequest;
import com.dp.blackhole.protocol.data.ProduceRequest;
import com.dp.blackhole.protocol.data.RegisterRequest;
import com.dp.blackhole.protocol.data.RotateRequest;
import com.dp.blackhole.storage.FileMessageSet;
import com.dp.blackhole.storage.MessageAndOffset;
import com.dp.blackhole.storage.MessageSet;

public class BrokerService extends Thread {
    private final Log LOG = LogFactory.getLog(BrokerService.class);
    
    GenServer<TransferWrap, DelegationIOConnection, EntityProcessor<TransferWrap, DelegationIOConnection>> server;
    StorageManager manager;
    PublisherExecutor executor;
    private Map<DelegationIOConnection, ClientDesc> clients;
    int servicePort;
    int numHandler;
    
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
        String storagedir = prop.getProperty("broker.storage.dir");
        int splitThreshold = Integer.parseInt(prop.getProperty("broker.storage.splitThreshold", "536870912"));
        int flushThreshold = Integer.parseInt(prop.getProperty("broker.storage.flushThreshold", "4194304"));
        numHandler = Integer.parseInt(prop.getProperty("GenServer.handler.count", "3"));
        servicePort = Integer.parseInt(prop.getProperty("broker.service.port"));
        clients = new ConcurrentHashMap<DelegationIOConnection, BrokerService.ClientDesc>();
        manager = new StorageManager(storagedir, splitThreshold, flushThreshold);
        executor = new PublisherExecutor();
        ConnectionFactory<DelegationIOConnection> factory = new DelegationIOConnection.DelegationIOConnectionFactory();
        TypedFactory wrappedFactory = new DataMessageTypeFactory();
        server = new GenServer<TransferWrap, DelegationIOConnection, EntityProcessor<TransferWrap, DelegationIOConnection>>
            (executor, factory, wrappedFactory);
    }
    
    public PublisherExecutor getExecutor() {
        return executor;
    }
    
    public StorageManager getPersistentManager() {
        return manager;
    }
    
    public void disconnectClients() {
        for (DelegationIOConnection client : clients.keySet()) {
            server.closeConnection(client);
        }
    }
    
    public class PublisherExecutor implements
            EntityProcessor<TransferWrap, DelegationIOConnection> {
        
        private void closeClientOfErrorRequest(DelegationIOConnection from, Object request) {
            LOG.info("can't find topic/partition with " + request + " from " + from +" ,close connection");
            server.closeConnection(from);
        }
        
        public void handleProduceRequest(ProduceRequest request,
                DelegationIOConnection from) {
            try {
                Partition p = manager.getPartition(request.topic,
                        request.partitionId, true);
                p.append(request.getMesssageSet());
            } catch (IOException e) {
                LOG.error("IOE catched", e);
            }
        }

        public void handleFetchRequest(FetchRequest request,
                DelegationIOConnection from) {
            Partition p = null;
            FileMessageSet messages = null;
            try {
                p = manager.getPartition(request.topic, request.partitionId, false);
                if (p == null) {
                    closeClientOfErrorRequest(from, request);
                    return;
                }
                messages = p.read(request.offset, request.limit);
            } catch (IOException e) {
                LOG.error("IOE catched", e);
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
                    p = manager.getPartition(f.topic, f.partitionId, false);
                    if (p == null) {
                        closeClientOfErrorRequest(from, request);
                        return;
                    }
                    messages = p.read(f.offset, f.limit);
                } catch (IOException e) {
                    LOG.error("IOE catched", e);
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
                p = manager.getPartition(request.topic, request.partition, false);
                if (p == null) {
                    closeClientOfErrorRequest(from, request);
                    return;
                }
            } catch (IOException e) {
                LOG.error("IOE catched", e);
            }
            from.send(new TransferWrap(new OffsetReply(request.topic, request.partition, p.getEndOffset())));
        }
        
        public void handleRotateRequest(RotateRequest request, DelegationIOConnection from) {
            Partition p = null;
            try {
                p = manager.getPartition(request.topic, request.partitionId, false);
                if (p == null) {
                    closeClientOfErrorRequest(from, request);
                    return;
                }
                RollPartition roll = p.markRotate();
                Broker.getRollMgr().doRegister(request.topic, request.partitionId, request.rollPeriod, roll);
            } catch (IOException e) {
                LOG.error("Got an IOE", e);
            }
        }
        
        public void handleRegisterRequest(RegisterRequest request, DelegationIOConnection from) {
            clients.put(from, new ClientDesc(request.topic, ClientDesc.AGENT, request.source));
            try {
                manager.getPartition(request.topic, request.source, true);
            } catch (IOException e) {
                LOG.error("Got an IOE", e);
            }
            Message msg = PBwrap.wrapReadyBroker(request.topic, request.source, request.peroid, request.broker, Util.getTS());
            Broker.getSupervisor().send(msg);
        }
        
        public void handleLastRotateRequest(LastRotateRequest request, DelegationIOConnection from) {
            Partition p = null;
            try {
                p = manager.getPartition(request.topic, request.partitionId, false);
                if (p == null) {
                    closeClientOfErrorRequest(from, request);
                    return;
                }
                RollPartition roll = p.markRotate();
                Broker.getRollMgr().doClean(request.topic, request.partitionId, request.rollPeriod, roll);
            } catch (IOException e) {
                LOG.error("Got an IOE", e);
            }
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
            case DataMessageTypeFactory.LastRotateRequest:
                handleLastRotateRequest((LastRotateRequest) request.unwrap(), from);
                break;
            default:
                LOG.error("unknown message type: " + request.getType());
            }
        }

        @Override
        public void OnConnected(DelegationIOConnection connection) {
            LOG.info(connection + " connected");
            clients.put(connection, new ClientDesc(ClientDesc.CONSUMER));
        }

        @Override
        public void OnDisconnected(DelegationIOConnection connection) {
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

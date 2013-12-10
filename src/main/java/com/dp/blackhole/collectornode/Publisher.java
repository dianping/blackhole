package com.dp.blackhole.collectornode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.collectornode.persistent.FileMessageSet;
import com.dp.blackhole.collectornode.persistent.MessageAndOffset;
import com.dp.blackhole.collectornode.persistent.MessageSet;
import com.dp.blackhole.collectornode.persistent.Partition;
import com.dp.blackhole.collectornode.persistent.PersistentManager;
import com.dp.blackhole.collectornode.persistent.PersistentManager.reporter.ReportEntry;
import com.dp.blackhole.collectornode.persistent.protocol.FetchReply;
import com.dp.blackhole.collectornode.persistent.protocol.FetchRequest;
import com.dp.blackhole.collectornode.persistent.protocol.DataMessageTypeFactory;
import com.dp.blackhole.collectornode.persistent.protocol.MultiFetchReply;
import com.dp.blackhole.collectornode.persistent.protocol.MultiFetchRequest;
import com.dp.blackhole.collectornode.persistent.protocol.OffsetReply;
import com.dp.blackhole.collectornode.persistent.protocol.OffsetRequest;
import com.dp.blackhole.collectornode.persistent.protocol.ProduceRequest;
import com.dp.blackhole.network.ConnectionFactory;
import com.dp.blackhole.network.DelegationIOConnection;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenServer;
import com.dp.blackhole.network.TransferWrap;
import com.dp.blackhole.network.TypedFactory;

public class Publisher extends Thread {
    private final Log Log = LogFactory.getLog(Publisher.class);
    
    GenServer<TransferWrap, DelegationIOConnection, EntityProcessor<TransferWrap, DelegationIOConnection>> server;
    PublisherExecutor executor;
    Properties prop;
    
    public static void reportPartitionInfo(List<ReportEntry> entrylist) {
        Collectornode.getCollectorNode().reportPartitionInfo(entrylist);
    }
    
    @Override
    public void run() {
        try {
            server.init(prop, "Publisher");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public Publisher(Properties prop) throws IOException {
        this.prop = prop;
        String storagedir = prop.getProperty("publisher.storage.dir");
        int splitThreshold = Integer.parseInt(prop.getProperty("publisher.storage.splitThreshold", "536870912"));
        int flushThreshold = Integer.parseInt(prop.getProperty("publisher.storage.flushThreshold", "4194304"));
        PersistentManager mananger = new PersistentManager(storagedir, splitThreshold, flushThreshold);
        executor = new PublisherExecutor(mananger);
        ConnectionFactory<DelegationIOConnection> factory = new DelegationIOConnection.DelegationIOConnectionFactory();
        TypedFactory wrappedFactory = new DataMessageTypeFactory();
        server = new GenServer<TransferWrap, DelegationIOConnection, EntityProcessor<TransferWrap, DelegationIOConnection>>
            (executor, factory, wrappedFactory);
    }
    
    public PublisherExecutor getExecutor() {
        return executor;
    }
    
    public class PublisherExecutor implements
            EntityProcessor<TransferWrap, DelegationIOConnection> {
        PersistentManager mananger;

        public PublisherExecutor(PersistentManager mananger) {
            this.mananger = mananger;
        }

        public void handleProduceRequest(ProduceRequest request,
                DelegationIOConnection from) {
            try {
                Partition p = mananger.getPartition(request.topic,
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
                p = mananger.getPartition(request.topic, request.partitionId);
                messages = p.read(request.offset, request.limit);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            if (messages == null) {
                Log.info("request offset: " + request.offset + "current tail offset: " + p.getEndOffset());
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
                    p = mananger.getPartition(f.topic, f.partitionId);
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
                p = mananger.getPartition(request.topic, request.partition);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            from.send(new TransferWrap(new OffsetReply(request.topic, request.partition, p.getEndOffset())));
        }
        
        @Override
        public void process(TransferWrap request, DelegationIOConnection from) {
            switch (request.getType()) {
            case DataMessageTypeFactory.MultiFetchRequest:
                handleMultiFetchRequest((MultiFetchRequest) request.unwrap(),
                        from);
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
            }
        }

        @Override
        public void OnConnected(DelegationIOConnection connection) {
            // TODO Auto-generated method stub

        }

        @Override
        public void OnDisconnected(DelegationIOConnection connection) {
            // TODO Auto-generated method stub

        }

    }

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("GenServer.handlercount", "1");
        properties.setProperty("GenServer.port", "2222");
        properties.setProperty("publisher.storage.dir", "/tmp/base");
        Publisher pubservice = new Publisher(properties);
        pubservice.run();
    }
}

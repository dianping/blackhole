package com.dp.blackhole.supervisor;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.lion.client.LionException;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.datachecker.AbnormalStageChecker;
import com.dp.blackhole.datachecker.Checkpoint;
import com.dp.blackhole.http.RequestListener;
import com.dp.blackhole.network.NonblockingConnectionFactory;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenServer;
import com.dp.blackhole.network.ByteBufferNonblockingConnection;
import com.dp.blackhole.protocol.control.AppRegPB.AppReg;
import com.dp.blackhole.protocol.control.AssignBrokerPB.AssignBroker;
import com.dp.blackhole.protocol.control.AssignConsumerPB.AssignConsumer;
import com.dp.blackhole.protocol.control.BrokerRegPB.BrokerReg;
import com.dp.blackhole.protocol.control.CommonConfResPB.CommonConfRes;
import com.dp.blackhole.protocol.control.ConfReqPB.ConfReq;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes.AppConfRes;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes.LxcConfRes;
import com.dp.blackhole.protocol.control.ConsumerExitPB.ConsumerExit;
import com.dp.blackhole.protocol.control.ConsumerRegPB.ConsumerReg;
import com.dp.blackhole.protocol.control.DumpAppPB.DumpApp;
import com.dp.blackhole.protocol.control.DumpConsumerGroupPB.DumpConsumerGroup;
import com.dp.blackhole.protocol.control.FailurePB.Failure;
import com.dp.blackhole.protocol.control.HeartbeatPB.Heartbeat;
import com.dp.blackhole.protocol.control.LogNotFoundPB.LogNotFound;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.OffsetCommitPB.OffsetCommit;
import com.dp.blackhole.protocol.control.PartitionRequireBrokerPB.PartitionRequireBroker;
import com.dp.blackhole.protocol.control.ProducerRegPB.ProducerReg;
import com.dp.blackhole.protocol.control.ReadyStreamPB.ReadyStream;
import com.dp.blackhole.protocol.control.ReadyUploadPB.ReadyUpload;
import com.dp.blackhole.protocol.control.RemoveConfPB.RemoveConf;
import com.dp.blackhole.protocol.control.RestartPB.Restart;
import com.dp.blackhole.protocol.control.RetirePB.Retire;
import com.dp.blackhole.protocol.control.RollCleanPB.RollClean;
import com.dp.blackhole.protocol.control.RollIDPB.RollID;
import com.dp.blackhole.protocol.control.SnapshotOpPB.SnapshotOp.OP;
import com.dp.blackhole.protocol.control.TopicReportPB.TopicReport;
import com.dp.blackhole.protocol.control.TopicReportPB.TopicReport.TopicEntry;
import com.dp.blackhole.rest.HttpServer;
import com.dp.blackhole.rest.ServiceFactory;
import com.dp.blackhole.supervisor.model.BrokerDesc;
import com.dp.blackhole.supervisor.model.ConnectionDesc;
import com.dp.blackhole.supervisor.model.ConnectionDesc.ConnectionInfo;
import com.dp.blackhole.supervisor.model.ConsumerDesc;
import com.dp.blackhole.supervisor.model.ConsumerGroup;
import com.dp.blackhole.supervisor.model.ConsumerGroupKey;
import com.dp.blackhole.supervisor.model.Issue;
import com.dp.blackhole.supervisor.model.NodeDesc;
import com.dp.blackhole.supervisor.model.ProducerManager;
import com.dp.blackhole.supervisor.model.PartitionInfo;
import com.dp.blackhole.supervisor.model.ProducerDesc;
import com.dp.blackhole.supervisor.model.Stage;
import com.dp.blackhole.supervisor.model.Stream;
import com.dp.blackhole.supervisor.model.Topic;
import com.dp.blackhole.supervisor.model.TopicConfig;
import com.google.protobuf.InvalidProtocolBufferException;

public class Supervisor {

    public static final Log LOG = LogFactory.getLog(Supervisor.class);
    private ConfigManager configManager;
    
    private GenServer<ByteBuffer, ByteBufferNonblockingConnection, EntityProcessor<ByteBuffer, ByteBufferNonblockingConnection>> server;  
    private ConcurrentHashMap<ByteBufferNonblockingConnection, ArrayList<Stream>> connectionStreamMap;
    private ConcurrentHashMap<Stage, ByteBufferNonblockingConnection> stageConnectionMap;

    private ConcurrentHashMap<String, Topic> topics;

  
    private ConcurrentHashMap<ConsumerGroupKey, ConsumerGroup> consumerGroups;
    private ConcurrentHashMap<ByteBufferNonblockingConnection, ConnectionDesc> connections;
    //agent hostname -> connection or producerId -> connection
    private ConcurrentHashMap<String, ByteBufferNonblockingConnection> dataSourceMapping;
    private ConcurrentHashMap<String, ByteBufferNonblockingConnection> brokersMapping;
    private ConcurrentHashMap<String, ProducerManager> producerMgrMap; // topic -> manager
    
    public Checkpoint checkpoint;

    public Set<String> getAllTopicNames() {
        return new HashSet<String>(topics.keySet());
    }
    
    public Set<ConsumerGroupKey> getAllConsumerGroupKeys() {
        return new HashSet<ConsumerGroupKey>(consumerGroups.keySet());
    }
    
    public ConsumerGroup getConsumerGroup(ConsumerGroupKey groupKey) {
        return consumerGroups.get(groupKey);
    }
    
    public Set<ConsumerGroup> getCopyOfConsumerGroups() {
        Set<ConsumerGroup> copy = new HashSet<ConsumerGroup>(consumerGroups.size());
        copy.addAll(consumerGroups.values());
        return copy;
    }
    
    public boolean isActiveStream(String topic, String source) {
        Stream stream = getStream(topic, source);
        return stream == null ? false : stream.isActive();
    }
    
    public boolean isEmptyStream(String topic, String source) {
        Stream stream = getStream(topic, source);
        if (stream == null) {
            return true;
        }
        List<Stage> stages = stream.getStages();
        return ((stages == null || stages.size() == 0) ? true : false);
    }
    
    public boolean isCleanStream(String topic, String source) {
        Topic t = topics.get(topic);
        if (t == null) {
            return true;
        }
        Stream stream = t.getStream(source);
        return ((stream == null) ? true : false);
    }

    private void send(ByteBufferNonblockingConnection connection, Message msg) {
        if (connection == null || !connection.isActive()) {
            LOG.error("connection is null or closed, message sending abort: " + msg);
            return;
        }
        switch (msg.getType()) {
        case NO_AVAILABLE_CONF:
        case DUMP_APP:
        case DUMP_CONF:
        case DUMP_STAT:
        case DUMP_REPLY:
            break;
        default:
            LOG.debug("send message to " + connection + " :" +msg);
        }
        Util.send(connection, msg);
    }
    
    public void cachedSend(Map<String, Message> toBeSend) {
        for (Map.Entry<String, Message> entry : toBeSend.entrySet()) {
            cachedSend(entry.getKey(), entry.getValue());
        }
    }
    
    public void cachedSend(String host, Message toBeSend) {
        ByteBufferNonblockingConnection agent = getDataSourceConnectionByHostname(host);
        if (agent == null) {
            LOG.info("Can not find any agents connected by " + host + ", message send abort.");
            return;
        }
        send(agent, toBeSend);
    }
    
    void findAndSendAppConfRes(TopicConfig confInfo) {
        List<String> hosts = confInfo.getHosts();
        if (hosts == null || hosts.isEmpty()) {
            LOG.error("Not found any hosts for " + confInfo.getTopic() + ", it's abnormal.");
            return;
        }
        List<AppConfRes> appConfResList = new ArrayList<AppConfRes>(1);
        AppConfRes appConfRes = PBwrap.wrapAppConfRes(
                confInfo.getTopic(),
                confInfo.getWatchLog(),
                String.valueOf(confInfo.getRotatePeriod()),
                String.valueOf(confInfo.getRollPeriod()),
                String.valueOf(confInfo.getMaxLineSize()),
                String.valueOf(confInfo.getReadInterval()),
                String.valueOf(confInfo.getMinMsgSent()),
                String.valueOf(confInfo.getMsgBufSize()),
                String.valueOf(confInfo.getBandwidthPerSec()),
                confInfo.getTailPosition()
        );
        appConfResList.add(appConfRes);
        Message message = PBwrap.wrapConfRes(appConfResList, null);
        for (String agentHost : hosts) {
            //we first find the connections from datasourceMapping cause its confReq-conRes message loop is termination.
            ByteBufferNonblockingConnection connection = getDataSourceConnectionByHostname(agentHost);
            if (connection == null) {
                connection = getIdleConnectionByHostname(agentHost);
            }
            if (connection != null) {
                send(connection, message);
            } else {
                LOG.warn("Can not get connection by agenthost" + agentHost);
            }
        }
    }
    

    void findAndSendLxcConfRes(TopicConfig confInfo) {
        Map<String, Set<String>> hostToInstances = confInfo.getInstances();
        if (hostToInstances == null || hostToInstances.isEmpty()) {
            return;
        }
        Map<String, Message> toBeSend = new HashMap<String, Message>();
        for(Map.Entry<String, Set<String>> entry : hostToInstances.entrySet()) {
            String eachHost = entry.getKey();
            Set<String> idsInTheSameHost = entry.getValue();
            List<LxcConfRes> lxcConfResList = new ArrayList<LxcConfRes>();
            if (idsInTheSameHost.size() == 0) {
                continue;
            }
            LxcConfRes lxcConfRes = PBwrap.wrapLxcConfRes(
                    confInfo.getTopic(),
                    confInfo.getWatchLog(),
                    String.valueOf(confInfo.getRotatePeriod()),
                    String.valueOf(confInfo.getRollPeriod()),
                    String.valueOf(confInfo.getMaxLineSize()),
                    String.valueOf(confInfo.getReadInterval()),
                    String.valueOf(confInfo.getMinMsgSent()),
                    String.valueOf(confInfo.getMsgBufSize()),
                    String.valueOf(confInfo.getBandwidthPerSec()),
                    confInfo.getTailPosition(),
                    idsInTheSameHost);
            lxcConfResList.add(lxcConfRes);
            Message message = PBwrap.wrapConfRes(null, lxcConfResList);
            toBeSend.put(eachHost, message);
        }
        if (toBeSend != null) {
            cachedSend(toBeSend);
        }
    }
    
    private void handleHeartBeat(Message msg, ByteBufferNonblockingConnection from) {
        ConnectionDesc desc = connections.get(from);
        if (desc == null) {
            LOG.error("can not find ConnectionDesc by connection " + from);
            return;
        }
        desc.updateHeartBeat();
        try {
            Heartbeat heartbeat = msg.getHeartbeat();
            desc.setVersion(heartbeat.getVersion());
        } catch (Exception e) {
            //Compatible with the old version
        }
    }
    
    private void addStream(Stream stream) {
        Topic t = topics.get(stream.getTopic());
        if (t != null) {
            t.addStream(stream);
        }
    }
    
    public Stream getStream(String topic, String source) {
        Stream stream = null;
        Topic t = topics.get(topic);
        if (t != null) {
            stream = t.getStream(source);
        }
        return stream;
    }
    
    public List<Stream> getAllStreams(String topic) {
        List<Stream> streams;
        Topic t = topics.get(topic);
        if (t != null) {
            streams = t.getAllStreamsOfCopy();
        } else {
            streams = new ArrayList<Stream>();
        }
        return streams;
    }
    
    @SuppressWarnings("unused")
    private void addPartition(String topic, PartitionInfo pInfo) {
        Topic t = topics.get(topic);
        if (t != null) {
            t.addPartition(pInfo.getId(), pInfo);
        }
    }
    
    private void removeStream(String topic, String source) {
        Topic t = topics.get(topic);
        if (t != null) {
            t.removeStream(source);
            //remove this topic if there is no stream
            if (t.getAllStreamsOfCopy().size() == 0) {
                topics.remove(topic);
            }
        }
    }
    
    private PartitionInfo getPartition(String topic, String partitionId) {
        PartitionInfo pInfo = null;
        Topic t = topics.get(topic);
        if (t != null) {
            pInfo = t.getPartition(partitionId);
        }
        return pInfo;
    }
    
    @SuppressWarnings("unused")
    private void removePartition(String topic, String partitionId) {
        Topic t = topics.get(topic);
        if (t != null) {
            t.removePartition(partitionId);
        }
    }
    
    private void handleTopicReport(TopicReport report, ByteBufferNonblockingConnection from) { 
        for (TopicEntry entry : report.getEntriesList()) {
            String topic = entry.getTopic();
            String partitionId = entry.getPartitionId();
            
            // update partition offset
            PartitionInfo partitionInfo = getPartition(topic, partitionId);
            if (partitionInfo == null) {
                LOG.warn("partition: " + topic + "." + partitionId +" can't be found, while exists in topic report");
                continue;
            } else {
                if (partitionInfo.isOffline()) {
                    continue;
                }
                partitionInfo.setEndOffset(entry.getOffset());
            }
        }
    }
    
    private void sendConsumerRegFail(ByteBufferNonblockingConnection from, String group, String consumerId, String topic) {
        Message message = PBwrap.wrapConsumerRegFail(group, consumerId, topic);
        send(from, message);
    }
    
    private void handleConsumerReg(ConsumerReg consumerReg, ByteBufferNonblockingConnection from) {
        ConnectionDesc desc = connections.get(from);
        if (desc == null) {
            LOG.error("can not find ConnectionDesc by connection " + from);
            return;
        }
        desc.setType(ConnectionDesc.CONSUMER);
        
        String groupId = consumerReg.getGroupId();
        String consumerId = consumerReg.getConsumerId();
        String topic = consumerReg.getTopic();

        ArrayList<PartitionInfo> availPartitions = getAvailPartitions(topic);
        if (availPartitions == null) {
            LOG.error("unknown topic: " + topic);
            sendConsumerRegFail(from, groupId, consumerId, topic);
            return;
        } else if (availPartitions.size() == 0) {
            LOG.error("no partition available , topic: " + topic);
            sendConsumerRegFail(from, groupId, consumerId, topic);
            return;
        } 
        
        ConsumerGroupKey groupKey = new ConsumerGroupKey(groupId, topic);
        ConsumerGroup group = consumerGroups.get(groupKey);
        if (group == null) {
            group = new ConsumerGroup(groupKey);
            consumerGroups.put(groupKey, group);
        }
        
        ConsumerDesc consumerDesc = new ConsumerDesc(consumerId, groupId, topic, from);
        desc.attach(consumerDesc);
        
        tryAssignConsumer(consumerDesc, group);
    }
    
    public void tryAssignConsumer(ConsumerDesc consumer, ConsumerGroup group) {
        synchronized (group) {
            List<ConsumerDesc> consumes = group.getConsumes();
            if (consumes == null) {
                consumes = new ArrayList<ConsumerDesc>();
            }
            // new consumer arrived?
            if (consumer != null) {
                if (group.exists(consumer)) {
                    LOG.error("consumer already exists: " + consumer);
                    return;
                }
                consumes.add(consumer);
            }
            if (consumes.isEmpty()) {
                return;
            }
            Collections.shuffle(consumes);
            
            // get online partitions
            ArrayList<PartitionInfo> partitions = getAvailPartitions(group.getTopic());
            if (partitions == null || partitions.size() ==0) {
                LOG.error("can not get any available partitions");
                return;
            }
            
            // split partitions by consumer number
            int consumerNum = consumes.size();
            ArrayList<ArrayList<PartitionInfo>> assignPartitions
                = new ArrayList<ArrayList<PartitionInfo>>(consumerNum);
            for (int i = 0; i < consumerNum; i++) {
                assignPartitions.add(new ArrayList<PartitionInfo>());
            }
            
            // split partition into consumerNum groups
            for (int i = 0; i < partitions.size(); i++) {
                PartitionInfo pinfo = partitions.get(i);
                assignPartitions.get(i % consumerNum).add(pinfo);
            }
            
            for (int i = 0; i < consumerNum; i++) {
                ConsumerDesc cond = consumes.get(i);
                ArrayList<PartitionInfo> pinfoList = assignPartitions.get(i);
                List<AssignConsumer.PartitionOffset> offsets = new ArrayList<AssignConsumer.PartitionOffset>(pinfoList.size());
                for (PartitionInfo info : pinfoList) {
                    String broker = info.getHost()+ ":" + getBrokerPort(info.getHost());
                    long endOffset = info.getEndOffset();
                    long committedOffset = group.getCommittedOffsetByParitionId(info.getId());
                    AssignConsumer.PartitionOffset offset = PBwrap.getPartitionOffset(broker, info.getId(), endOffset, committedOffset);
                    offsets.add(offset);
                }
                Message assign = PBwrap.wrapAssignConsumer(group.getGroupId(), cond.getId(), group.getTopic(), offsets);
                send(cond.getConnection(), assign);
            }
            
            // update consumerGroup mapping
            group.update(consumes, assignPartitions, partitions);
        }
    }

    private ArrayList<PartitionInfo> getAvailPartitions(String topic) {
        ArrayList<PartitionInfo> availPartitions = null;
        Topic t = topics.get(topic);
        if (t != null) {
            List<PartitionInfo> partitions = t.getAllPartitionsOfCopy();
            if (partitions != null) {
                availPartitions = new ArrayList<PartitionInfo>();
                for (PartitionInfo pinfo : partitions) {
                    if (pinfo.isOffline()) {
                        continue;
                    }
                    availPartitions.add(pinfo);
                }
            }
        }
        return availPartitions;
    }
    
    private void handleOffsetCommit(OffsetCommit offsetCommit) {
        String id = offsetCommit.getConsumerIdString();
        String groupId = offsetCommit.getGroupId();
        if (groupId == null || groupId.length() == 0) {
            groupId = id.split("-")[0];
        }
        String topic = offsetCommit.getTopic();
        String partition = offsetCommit.getPartition();
        long offset = offsetCommit.getOffset();
        
        ConsumerGroupKey groupKey = new ConsumerGroupKey(groupId, topic);
        ConsumerGroup group = consumerGroups.get(groupKey);
        if (group == null) {
            LOG.warn("can not find consumer group " + groupKey);
            return;
        }
        
        group.updateOffset(id, topic, partition, offset);//TODO consumerid to consumer
    }
    
    private void markPartitionOffline(String topic, String partitionId) {
        PartitionInfo pinfo = getPartition(topic, partitionId);
        if (pinfo == null) {
            LOG.warn("can't find partition by partition: " + topic + "." + partitionId);
            return;
        }
        pinfo.markOffline(true);
        LOG.info(pinfo + " is offline");
    }
    
    /*
     * mark the stream as inactive, mark all the stages as pending unless the uploading stage
     * remove the relationship of the corresponding broker and streams
     */
    private void handleAppNodeFail(ConnectionDesc desc, long now) {
        ByteBufferNonblockingConnection connection = desc.getConnection();
        ArrayList<Stream> streams = connectionStreamMap.get(connection);
        if (streams != null) {
            for (Stream stream : streams) {
                LOG.info("mark stream as inactive: " + stream);
                stream.updateActive(false);
                List<Stage> stages = stream.getStages();
                if (stages != null) {
                    synchronized (stages) {
                        for (Stage stage : stages) {
                            LOG.info("checking stage: " + stage);
                            if (stage.getStatus() != Stage.UPLOADING) {
                                Issue e = new Issue();
                                e.setDesc("logreader failed");
                                e.setTs(now);
                                stage.getIssuelist().add(e);
                                stage.setStatus(Stage.PENDING);
                            }
                        }
                    }
                }
                
                // remove corresponding broker's relationship with the stream
                String brokerHost = stream.getBrokerHost();
                ByteBufferNonblockingConnection brokerConnection = brokersMapping.get(brokerHost);
                if (brokerConnection != null) {
                    ArrayList<Stream> associatedStreams = connectionStreamMap.get(brokerConnection);
                    if (associatedStreams != null) {
                        synchronized (associatedStreams) {
                            associatedStreams.remove(stream);
                            if (associatedStreams.size() == 0) {
                                connectionStreamMap.remove(brokerConnection);
                            }
                        }
                    } 
                }
                
                // mark partitions as offline
                String topic = stream.getTopic();
                String partitionId = stream.getSource();
                markPartitionOffline(topic, partitionId);
            }
        } else {
            LOG.warn("can not get associate streams from connectionStreamMap by connection: " + connection);
        }
    }
    
    /*
     * 1. process current stage on associated streams
     * 2. remove the relationship of the corresponding agent and streams
     * 3. processing uploading and recovery stages
     */
    private void handleBrokerNodeFail(ConnectionDesc desc, long now) {
        ByteBufferNonblockingConnection connection = desc.getConnection();
        ArrayList<Stream> streams = connectionStreamMap.get(connection);
        // processing current stage on streams
        if (streams != null) {
            for (Stream stream : streams) {
                //change the status of current stage if stream's host equals to the connection's
                if (stream.getBrokerHost().equals(connection.getHost())) {
                    List<Stage> stages = stream.getStages();
                    if (stages.size() == 0) {
                        continue;
                    }
                    synchronized (stages) {
                        Stage current = stages.get(stages.size() -1);
                        if (!current.isCurrent()) {
                            LOG.error("stage " + current + "should be current stage");
                            continue;
                        }
                        LOG.info("checking current stage: " + current);
                        
                        Issue e = new Issue();
                        e.setDesc("broker failed");
                        e.setTs(now);
                        current.getIssuelist().add(e);
    
                        // do not reassign broker here, since logreader will find broker fail,
                        // and do appReg again; otherwise two appReg for the same stream will send 
                        if (brokersMapping.size() == 0) {
                            current.setStatus(Stage.PENDING);
                        } else {
                            current.setStatus(Stage.BROKERFAIL);
                        }
                        LOG.info("after checking current stage: " + current);
                    }
                }
                // remove corresponding appNodes's relationship with the stream
                String dataSourceHost = Util.getHostFromSource(stream.getSource());
                ByteBufferNonblockingConnection dataSourceConnection = dataSourceMapping.get(dataSourceHost);
                if (dataSourceConnection == null) {
                    LOG.error("can not find dataSourceConnection by host " + dataSourceHost);
                    continue;
                }
                ArrayList<Stream> associatedStreams = connectionStreamMap.get(dataSourceConnection);
                if (associatedStreams != null) {
                    synchronized (associatedStreams) {
                        associatedStreams.remove(stream);
                        if (associatedStreams.size() == 0) {
                            connectionStreamMap.remove(dataSourceConnection);
                        }
                    }
                }
                 
                // mark partitions as offline
                String topic = stream.getTopic();
                String partitionId = stream.getSource();
                markPartitionOffline(topic, partitionId);
            }
        } else {
            LOG.warn("can not get associate streams from connectionStreamMap by connection: " + connection);
        }
        
        // processing uploading and recovery stages
        for (Entry<Stage, ByteBufferNonblockingConnection> entry : stageConnectionMap.entrySet()) {
            if (connection.equals(entry.getValue())) {
                LOG.info("processing entry: "+ entry);
                Stage stage = entry.getKey();
                if (stage.getStatus() == Stage.PENDING) {
                    continue;
                }
                Topic t = topics.get(stage.getTopic());
                Stream stream = t.getStream(stage.getSource());
                if (stream == null) {
                    LOG.error("can not find stream by" + stage.getTopic() + ":" + stage.getSource());
                    continue;
                }
                List<Stage> stages = stream.getStages();
                if (stages != null) {
                    synchronized (stages) {
                        if (stream != null) {
                            doRecovery(stream, stage);
                        }
                    }
                }
            }
        }
    }
    

    private void handleConsumerFail(ConnectionDesc desc, long now) {
        ByteBufferNonblockingConnection connection = desc.getConnection();
        LOG.info("consumer " + connection + " disconnectted");
        
        List<NodeDesc> nodeDescs = desc.getAttachments();
        if (nodeDescs == null || nodeDescs.size() == 0) {
            return;
        }
        //the purpose of the map named 'toBeReAssign' is to avoid multiple distribution of consumers in a group
        HashMap<ConsumerGroupKey, ConsumerGroup> toBeReAssign = new HashMap<ConsumerGroupKey, ConsumerGroup>();
        for (NodeDesc nodeDesc : nodeDescs) {
            ConsumerDesc consumerDesc = (ConsumerDesc) nodeDesc;
            ConsumerGroupKey groupKey = new ConsumerGroupKey(consumerDesc.getGroupId(), consumerDesc.getTopic());
            ConsumerGroup group = consumerGroups.get(groupKey);
            if (group == null) {
                LOG.error("can not find groupDesc by ConsumerGroup: " + groupKey);
                continue;
            }

            group.unregisterConsumer(consumerDesc);
            
            if (group.getConsumerCount() != 0) {
                toBeReAssign.put(groupKey, group);
            } else {
                LOG.info("consumerGroup " + groupKey +" has not live consumer, thus be removed");
                toBeReAssign.remove(groupKey);
                consumerGroups.remove(groupKey);
            }
        }
        
        for (Map.Entry<ConsumerGroupKey, ConsumerGroup> entry : toBeReAssign.entrySet()) {
            ConsumerGroupKey key = entry.getKey();
            ConsumerGroup group = entry.getValue();
            LOG.info("reassign consumers in group key: " + key + ", caused by consumer fail: " + group);
            tryAssignConsumer(null, group);
        }
    }
    
    private void handleProducerNodeFail(ConnectionDesc desc, long now) {
        ByteBufferNonblockingConnection connection = desc.getConnection();
        LOG.info("producer node " + connection + " disconnectted");
        List<NodeDesc> nodeDescs = desc.getAttachments();
        if (nodeDescs == null || nodeDescs.size() == 0) {
            return;
        }
        for (NodeDesc nodeDesc : nodeDescs) {
            ProducerDesc producerDesc = (ProducerDesc) nodeDesc;
            String producerId = producerDesc.getId();
            String topic = producerDesc.getTopic();
            ProducerManager producerManager = producerMgrMap.get(topic);
            producerManager.inactive(producerId);
            dataSourceMapping.remove(producerId);
        }
        handleAppNodeFail(desc, now);
    }
    
    /*
     * cancel the key, remove it from agent or brokers, then revisit streams
     * 1. agent fail, mark the stream as inactive, mark all the stages as pending unless the uploading stage
     * 2. broker fail, reassign broker if it is current stage, mark the stream as pending when no available broker;
     *  do recovery if the stage is not current stage, mark the stage as pending when no available broker
     */
    private void closeConnection(ByteBufferNonblockingConnection connection) {
        LOG.info("close connection: " + connection);
        
        long now = Util.getTS();
        ConnectionDesc desc = connections.get(connection);
        if (desc == null) {
            LOG.error("can not find ConnectionDesc by connection " + connection);
            return;
        }
        String host = connection.getHost();
        switch (desc.getType()) {
        case ConnectionDesc.AGENT:
            dataSourceMapping.remove(host);
            LOG.info("close APPNODE: " + host);
            handleAppNodeFail(desc, now);
            break;
        case ConnectionDesc.BROKER:
            brokersMapping.remove(host);
            LOG.info("close BROKER: " + host);
            handleBrokerNodeFail(desc, now);
            break;
        case ConnectionDesc.CONSUMER:
            LOG.info("close consumer: " + host);
            handleConsumerFail(desc, now);
            break;
        case ConnectionDesc.PRODUCER:
            LOG.info("close producer: " + host);
            handleProducerNodeFail(desc, now);
            break;
        default:
            break;
        }
        synchronized (connections) {
            connections.remove(connection);
        }
        connectionStreamMap.remove(connection);
    }

    private void dumpstat(ByteBufferNonblockingConnection from) {
        StringBuilder sb = new StringBuilder();
        sb.append("dumpstat:\n");
        sb.append("############################## dump ##############################\n");
        
        sb.append("print Streams:\n");
        for (Topic t : topics.values()) {
            List<Stream> streams = t.getAllStreamsOfCopy();
            for (Stream stream : streams) {
                sb.append("[stream]\n")
                .append(stream)
                .append("\n")
                .append("[stages]\n");
                List<Stage> stages = stream.getStages();
                synchronized (stages) {
                    for (Stage stage : stages) {
                        sb.append(stage)
                        .append("\n");
                    }
                }
            }
        }
        sb.append("\n");
        sb.append("print stageConnectionMap:\n");
        for(Entry<Stage, ByteBufferNonblockingConnection> entry : stageConnectionMap.entrySet()) {
            sb.append("<")
            .append(entry.getKey())
            .append(", ")
            .append(entry.getValue())
            .append(">")
            .append("\n");
        }
        sb.append("\n");
        
        sb.append("print agents:\n");
        for(ByteBufferNonblockingConnection connection: dataSourceMapping.values()) {
            sb.append("<")
            .append(connection)
            .append(">")
            .append("\n");
        }
        sb.append("\n");
        
        sb.append("print brokers:\n");
        for(ByteBufferNonblockingConnection connection: brokersMapping.values()) {
            sb.append("<")
            .append(connection)
            .append(">")
            .append("\n");
        }
        sb.append("\n");
        
        sb.append("print connectionStreamMap:\n");
        for(Entry<ByteBufferNonblockingConnection, ArrayList<Stream>> entry : connectionStreamMap.entrySet()) {
            ByteBufferNonblockingConnection conn = entry.getKey();
            ConnectionDesc desc = connections.get(conn);
            if (desc != null) {
                sb.append(desc).append("\n");
            }
            ArrayList<Stream> streams = entry.getValue();
            synchronized (streams) {
                for (Stream stream : streams) {
                    sb.append(stream)
                    .append("\n");
                }
            }
            sb.append("\n");
        }
        
        sb.append("##################################################################");
        
        String dumpstat = sb.toString();
        Message message = PBwrap.wrapDumpReply(dumpstat);
        send(from, message);
    }
    
    private void dumpTopic(DumpApp dumpApp, ByteBufferNonblockingConnection from) {
        String topic = dumpApp.getTopic();
        StringBuilder sb = new StringBuilder();
        sb.append("dump topic:\n");
        sb.append("############################## dump ##############################\n");
        sb.append("print streamIdMap:\n");
        Topic t = topics.get(topic);
        if (t == null) {
            LOG.warn("Can not dump topic " + topic + ", cause no mapping exists.");
            return;
        }
        sb.append("\n");
        sb.append("print Streams:\n");
        for (Stream stream : t.getAllStreamsOfCopy()) {
            sb.append("[stream]\n")
            .append(stream)
            .append("\n").append("[partition]\n");
            PartitionInfo partitionInfo = getPartition(topic, stream.getSource());
            if (partitionInfo != null) {
                sb.append(partitionInfo).append("\n");
            }
            sb.append("[stages]\n");
            List<Stage> stages = stream.getStages();
            synchronized (stages) {
                for (Stage stage : stages) {
                    sb.append(stage);
                }
            }
        }
        sb.append("##################################################################");
        
        String dumpstat = sb.toString();
        Message message = PBwrap.wrapDumpReply(dumpstat);
        send(from, message);
    }
    
    public void dumpconf(ByteBufferNonblockingConnection from) {
        String dumpconf = configManager.dumpconf();
        Message message = PBwrap.wrapDumpReply(dumpconf);
        send(from, message);
    }

    public void dumpConsumerGroup(DumpConsumerGroup dumpConsumerGroup,
            ByteBufferNonblockingConnection from) {
        String topic = dumpConsumerGroup.getTopic();
        String groupId = dumpConsumerGroup.getGroupId();
        ConsumerGroupKey groupKey = new ConsumerGroupKey(groupId, topic);
        ConsumerGroup group = consumerGroups.get(groupKey);
        StringBuilder sb = new StringBuilder();
        if (group == null) {
            sb.append("Can not find consumer group by groupId:").append(groupId).append(" topic:").append(topic);
            LOG.info(sb.toString());
        } else {
            sb.append("dump consumer group:\n");
            sb.append("############################## dump ##############################\n");
            sb.append("print ").append(groupKey).append("\n");
            
            long sumDelta = 0;
            for(Map.Entry<String, AtomicLong> entry : group.getCommitedOffsets().entrySet()) {
                long delta = 0;
                PartitionInfo partitionInfo = getPartition(topic, entry.getKey());
                if (partitionInfo != null) {
                    delta = partitionInfo.getEndOffset() - entry.getValue().get();
                    sumDelta += delta;
                    sb.append(partitionInfo).append("\n");
                }
                sb.append("{committedinfo,").append(entry.getKey())
                .append(",").append(entry.getValue().get()).append(",").append(delta).append("}\n\n");
            }
            sb.append("The sum of slow offset delta [").append(sumDelta).append("]\n");
            sb.append("##################################################################");
        }
        Message message = PBwrap.wrapDumpReply(sb.toString());
        send(from, message);
    }

    public void listTopics(ByteBufferNonblockingConnection from) {
        StringBuilder sb = new StringBuilder();
        SortedSet<String> topicSet = new TreeSet<String>(topics.keySet());
        sb.append("list topics:\n");
        sb.append("############################## dump ##############################\n");
        for (String topicInOrder : topicSet) {
            sb.append("<")
            .append(topicInOrder)
            .append(">")
            .append("\n");
        }
        sb.append("##################################################################");
        
        String listApps = sb.toString();
        Message message = PBwrap.wrapDumpReply(listApps);
        send(from, message);
    }
    
    public List<ConnectionInfo> getConnectionInfoByVersion(String version) {
        List<ConnectionInfo> infos = new ArrayList<ConnectionInfo>();
        for(ConnectionDesc desc : connections.values()) {
            if(desc.getVersion().equalsIgnoreCase(version)) {
                infos.add(desc.getConnectionInfo());
            }
        }
        return infos;
    }
    
    public void listIdle(ByteBufferNonblockingConnection from) {
        StringBuilder sb = new StringBuilder();
        sb.append("list idle hosts:\n");
        sb.append("############################## dump ##############################\n");
        SortedSet<String> idleHosts = new TreeSet<String>();
        for(ConnectionDesc desc : connections.values()) {
            if (desc == null) {
                LOG.error("can not find ConnectionDesc by connection " + desc);
                return;
            }
            if (desc.getType() != ConnectionDesc.AGENT &&
                desc.getType() != ConnectionDesc.BROKER &&
                desc.getType() != ConnectionDesc.CONSUMER &&
                desc.getType() != ConnectionDesc.PRODUCER &&
                desc.getConnection() != from) {
                idleHosts.add(desc.getConnection().getHost());
            }
        }
        int count = 0;
        for (String idleHostInOrder : idleHosts) {
            count++;
            sb.append(idleHostInOrder).append("  ");
            if (count % 5 == 0) {
                sb.append("\n");
            }
        }
        sb.append("\n\n").append("idle hosts count: " + idleHosts.size());
        sb.append("\n").append("Total of connected hosts: " + connections.size());
        sb.append("\n").append("##################################################################");
        
        String listIdle = sb.toString();
        Message message = PBwrap.wrapDumpReply(listIdle);
        send(from, message);
    }
    
    public void listConsumerGroups(ByteBufferNonblockingConnection from) {
        StringBuilder sb = new StringBuilder();
        sb.append("list consumer groups:\n");
        sb.append("############################## dump ##############################\n");
        SortedSet<ConsumerGroupKey> groupsSorted  = new TreeSet<ConsumerGroupKey>(new Comparator<ConsumerGroupKey>() {
            @Override
            public int compare(ConsumerGroupKey o1, ConsumerGroupKey o2) {
                int topicResult = o1.getTopic().compareTo(o2.getTopic());
                return topicResult == 0 ? o1.getGroupId().compareTo(o2.getGroupId()) : topicResult;
            }
        });
        for (ConsumerGroupKey groupKey : consumerGroups.keySet()) {
            groupsSorted.add(groupKey);
        }
        for (ConsumerGroupKey groupKey : groupsSorted) {
            sb.append(groupKey).append("\n");
        }
        sb.append("##################################################################");
        
        String listConsGroup = sb.toString();
        Message message = PBwrap.wrapDumpReply(listConsGroup);
        send(from, message);
    }
    
    private void handleRestart(Restart restart) {
        List<String> agentServers = restart.getAgentServersList();
        sendRestart(agentServers);
    }

    public void sendRestart(List<String> agentServers) {
        for (String agentHost : agentServers) {
            ByteBufferNonblockingConnection agent = dataSourceMapping.get(agentHost);
            if (agent != null) {
                server.closeConnection(agent);
            } else {
                LOG.info("Can not find stream which from " + agentHost);
            }
        }
    }
    
    public boolean oprateSnapshot(String topic, String source, String opname) {
        ByteBufferNonblockingConnection c = dataSourceMapping.get(Util.getHostFromSource(source));
        if (c == null) {
            LOG.error("can not find connection by host: " + source);
            return false;
        }
        OP op;
        try {
            op = OP.valueOf(opname);
        } catch (Exception e) {
            LOG.error("Illegal opname: " + opname);
            op = OP.log;
        }
        Message message = PBwrap.wrapSnapshotOp(topic, source, op);
        send(c, message);
        return true;
    }

    public boolean pauseStream(String topic, String source, int delaySeconds) {
        Stream stream = getStream(topic, source);
        if (stream == null) {
            return false;
        }
        ByteBufferNonblockingConnection c = dataSourceMapping.get(Util.getHostFromSource(source));
        if (c == null) {
            LOG.error("can not find connection by host: " + source);
            return false;
        }
        stream.updateActive(false);
        List<Stage> stages = stream.getStages();
        for (Stage stage : stages) {
            if(stage.isCurrent()) {
                stage.setStatus(Stage.PAUSE);
            }
        }
        Message message = PBwrap.wrapPauseStream(topic, source, delaySeconds);
        send(c, message);
        return true;
    }

    public void removeConf(RemoveConf removeConf, ByteBufferNonblockingConnection from) {
        String topic = removeConf.getTopic();
        configManager.removeConf(topic);
    }

    private void handleRetireStream(Retire retire, ByteBufferNonblockingConnection from) {
        String topic = retire.getTopic();
        String source = retire.getSource();
        boolean force = retire.getForce();
        Stream stream = getStream(topic, source);
        retireStreamInternal(stream, force);
    }
    
    public boolean retireStream(String topic, String source) {
        Stream stream = getStream(topic, source);
        return retireStreamInternal(stream, false);
    }
    
    public boolean retireStream(String topic, String source, boolean force) {
        Stream stream = getStream(topic, source);
        return retireStreamInternal(stream, force);
    }
    
    private boolean retireStreamInternal(Stream stream, boolean forceRetire) {
        if (stream == null) {
            LOG.error("can't find stream");
            return false;
        }
        
        if (stream.isActive() && !forceRetire) {
            LOG.error("only inactive stream can be retired");
            return false;
        } else {
            if (brokersMapping.isEmpty()) {
                LOG.error("only inactive stream can be retired");
                return false;
            }
            
            LOG.info("retire stream: " + stream);
            
            // mark partitions as offline
            String topic = stream.getTopic();
            String source = stream.getSource();
            markPartitionOffline(topic, source);    //partitionId equals source
            
            // remove from streamIdMap
            removeStream(topic, source);
            
            // remove the stages from stageConnectionMap
            List<Stage> stages = stream.getStages();
            if (stages != null) {
                synchronized (stages) {
                    for (Stage stage : stages) {
                        stageConnectionMap.remove(stage);
                    }
                }
            } else  {
                LOG.error("can not find stages of stream: " + stream);
            }
            
            // remove stream from connectionStreamMap
            for (Entry<ByteBufferNonblockingConnection, ArrayList<Stream>> e : connectionStreamMap.entrySet()) {
                ArrayList<Stream> associatedStreams = e.getValue();
                synchronized (associatedStreams) {
                    associatedStreams.remove(stream);
                    if (associatedStreams.isEmpty()) {
                        connectionStreamMap.remove(e.getKey());
                    }
                }
            }
            // remove stream from Streams
            stream.setStages(new ArrayList<Stage>());
            return true;
        }
    }
    
    private void handleManualRecoveryRoll(RollID rollID) {
        manualRecoveryRoll(rollID.getTopic(), rollID.getSource(), rollID.getRollTs());
    }
    
    public boolean manualRecoveryRoll(String topic, String source, long rollTs) {
        Stream stream = getStream(topic, source);
        if (stream != null) {
            // check the stream is active
            if (!stream.isActive()) {
                LOG.error("the manual recovery stage must belong to an active stream");
                return false;
            }
            List<Stage> stages = stream.getStages();
            if (stages != null) {
                synchronized (stages) {
                    // process stage missed only
                    for (Stage stage : stages) {
                        if (stage.getRollTs() == rollTs) {
                            if (stage.getStatus() != Stage.RECOVERYING && stage.getStatus() != Stage.UPLOADING) {
                                doRecovery(stream, stage);
                            } else {
                                LOG.warn("Can't recovery stage manually cause the stage is " + stage.getStatus());
                            }
                            return true;
                        }
                    }
                    // create the stage
                    Stage manualRecoveryStage = new Stage();
                    manualRecoveryStage.setTopic(topic);
                    manualRecoveryStage.setSource(source);
                    manualRecoveryStage.setBrokerHost(null);
                    manualRecoveryStage.setCleanstart(false);
                    manualRecoveryStage.setIssuelist(new Vector<Issue>());
                    manualRecoveryStage.setStatus(Stage.RECOVERYING);
                    manualRecoveryStage.setRollTs(rollTs);
                    manualRecoveryStage.setCurrent(false);
                    // put the stage to head of the stages
                    ArrayList<Stage> newStages = new ArrayList<Stage>();
                    newStages.add(manualRecoveryStage);
                    newStages.addAll(stages);
                    stream.setStages(newStages);
                    
                    // do recovery
                    doRecovery(stream, manualRecoveryStage);
                }
                return true;
            } else {
                LOG.error("can not find stages of stream: " + stream);
                return false;
            }
        } else {
            LOG.error("can't find stream by " + topic + ":" + source);
            return false;
        }   
    }

    private void handleUnrecoverable(RollID rollID) {
        Stream stream = getStream(rollID.getTopic(), rollID.getSource());
        if (stream != null) {
            if (rollID.getIsFinal()) {
                LOG.info("Final but unrecoverable. Just retire this stream.");
                retireStreamInternal(stream, true);
                return;
            }
            List<Stage> stages = stream.getStages();
            if (stages != null) {
                synchronized (stages) {
                    for (Stage stage : stages) {
                        if (stage.getRollTs() == rollID.getRollTs()) {
                            LOG.info("handle unrecoverable stage " + stage);
                            String broker = getBrokerRandom();
                            if (broker != null) {
                                ByteBufferNonblockingConnection brokerConnection = brokersMapping.get(broker);
                                if (brokerConnection != null) {
                                    String topic = rollID.getTopic();
                                    String source = rollID.getSource();
                                    long rollTs = rollID.getRollTs();
                                    long period = rollID.getPeriod();
                                    Message message = PBwrap.wrapMarkUnrecoverable(topic, source, period, rollTs);
                                    send(brokerConnection, message);
                                }
                            } else {
                                LOG.error("Can not get any brokers when handle unrecoverable");
                            }
                            
                            stages.remove(stage);
                            stageConnectionMap.remove(stage);
                            //unrecoverable stage also need update lastSuccessTs
                            stream.setGreatlastSuccessTs(rollID.getRollTs());
                            break;
                        }
                    }
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        } else {
            LOG.error("can't find stream by " + rollID.getTopic() + ":" +rollID.getSource());
        }
        
    }
    
    /*
     * failure happened only on stream
     * just log it in a issue, because:
     * if it is a broker fail, the corresponding log reader should find it 
     * and ask for a new broker; if it is a agent fail, the agent will
     * register the topic again 
     */
    private void handleFailure(Failure failure) {
        Stream stream = getStream(failure.getTopic(), failure.getSource());
        if (stream == null) {
            LOG.error("can not find stream by " + failure.getTopic() + ":" +failure.getSource());
            return;
        }
        
        List<Stage> stages = stream.getStages();
        if (stages == null) {
            LOG.error("can not find stages of stream: " + stream);
            return;
        }
        
        synchronized (stages) {
            long failRollTs = Util.getCurrentRollTs(failure.getFailTs(), stream.getPeriod());
            Stage failstage = null;
            for (Stage s : stages) {
                if (s.getRollTs() == failRollTs) {
                    failstage = s;
                    break;
                }
            }
            // failure message may come before rolling message, and can be ignored here, 
            // since stream reestablish will mark the reconnect issue in current stage
            if (failstage == null) {
                LOG.error("failstage not found: " + failure);
                return;
            }
            Issue i = new Issue();
            i.setTs(failure.getFailTs());
            switch (failure.getType()) {
            case APP_NODE:
                i.setDesc("logreader failed");
                failstage.getIssuelist().add(i);
                break;
            case BROKER_NODE:
                i.setDesc("broker failed");
                failstage.getIssuelist().add(i);
                break;
            case PRODUCER:
                i.setDesc("producer failed");
                failstage.getIssuelist().add(i);
                break;
            default:
                LOG.error("Undefined node type!!");
                break;
            }
        }
    }
    
    /*
     * try to do recovery again when last recovery failed
     */
    private void handleRecoveryFail(RollID rollID) {
        Stream stream = getStream(rollID.getTopic(), rollID.getSource());
        if (stream != null) {
            List<Stage> stages = stream.getStages();
            if (stages != null) {
                synchronized (stages) {
                    for (Stage stage : stages) {
                        if (stage.getRollTs() == rollID.getRollTs()) {
                            doRecovery(stream, stage, rollID.getIsFinal());
                            break;
                        }
                    }
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        } else {
            LOG.error("can't find stream by " + rollID.getTopic() + ":" +rollID.getSource());
        }
    }

    /*
     * mark the stage as uploaded , print summary and remove it from Streams
     */
    private void handleRecoverySuccess(RollID rollID) {
        Stream stream = getStream(rollID.getTopic(), rollID.getSource());
        if (stream != null) {
            if (rollID.getIsFinal()) {
                LOG.info("Final upload suceessed. to retire this stream.");
                retireStreamInternal(stream, true);
                return;
            }
            stream.setGreatlastSuccessTs(rollID.getRollTs());
            List<Stage> stages = stream.getStages();
            if (stages != null) {
                synchronized (stages) {
                    for (Stage stage : stages) {
                        if (stage.getRollTs() == rollID.getRollTs()) {
                            stage.setStatus(Stage.UPLOADED);
                            LOG.info(stage);
                            stages.remove(stage);
                            stageConnectionMap.remove(stage);
                            break;
                        }
                    }
                }
            } else {
                LOG.warn("can not find stages of stream: " + stream);
            }
        } else {
            LOG.warn("can't find stream by " + rollID.getTopic() + ":" +rollID.getSource());
        }
    }

    /*
     * mark the upload failed stage as recovery
     * add issue to the stage, and do recovery
     */
    private void handleUploadFail(RollID rollID) {
        Stream stream = getStream(rollID.getTopic(), rollID.getSource());
        if (stream != null) {
            List<Stage> stages = stream.getStages();
            if (stages != null) {
                Issue e = new Issue();
                e.setDesc("upload failed");
                e.setTs(Util.getTS());
                synchronized (stages) {
                    for (Stage stage : stages) {
                        if (stage.getRollTs() == rollID.getRollTs()) {
                            stage.getIssuelist().add(e);
                            doRecovery(stream, stage, rollID.getIsFinal());
                            break;
                        }
                    }
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        } else {
            LOG.error("can't find stream by " + rollID.getTopic() + ":" +rollID.getSource());
        }
    }

    /*
     * update the stream's lastSuccessTs
     * make the uploaded stage as uploaded and remove it from Streams
     */
    private void handleUploadSuccess(RollID rollID, ByteBufferNonblockingConnection from) {
        Stream stream = getStream(rollID.getTopic(), rollID.getSource());
        if (stream != null) {
            if (rollID.getIsFinal()) {
                LOG.info("Final upload suceessed. to retire this stream.");
                retireStreamInternal(stream, true);
                return;
            }
            stream.setGreatlastSuccessTs(rollID.getRollTs()); 
       
            List<Stage> stages = stream.getStages();
            if (stages != null) {
                synchronized (stages) {
                    for (Stage stage : stages) {
                        if (stage.getRollTs() == rollID.getRollTs()) {
                            stage.setStatus(Stage.UPLOADED);
                            LOG.info(stage);
                            stages.remove(stage);
                            stageConnectionMap.remove(stage);
                            break;
                        }
                    }
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        } else {
            LOG.error("can't find stream by " + rollID.getTopic() + ":" +rollID.getSource());
        }
    }
    
    /*
     * current stage rolled
     * do recovery if current stage is not clean start or some issues happpened,
     * or upload the rolled stage;
     * create next stage as new current stage
     */
    private void handleRolling(ReadyUpload readyUpload, ByteBufferNonblockingConnection from) {
        Stream stream = getStream(readyUpload.getTopic(), readyUpload.getSource());
        if (stream != null) {
            if (readyUpload.getRollTs() <= stream.getLastSuccessTs()) {
                LOG.error("Receive a illegal roll ts (" + readyUpload.getRollTs() + ") from broker(" + from.getHost() + ")");
                return;
            }
            List<Stage> stages = stream.getStages();
            if (stages != null) {
                synchronized (stages) {
                    Stage current = null;
                    for (Stage stage : stages) {
                        if (stage.getRollTs() == readyUpload.getRollTs()) {
                            current = stage;
                        }
                    }
                    if (current == null) {
                        if (stages.size() > 0) {
                            LOG.warn("Stages may missed from stage:" + stages.get(stages.size() - 1)
                                    + " to stage:" + readyUpload.getRollTs());
                        } else {
                            LOG.warn("There are no stages in stream " + stream);
                        }
                        int missedStageCount = getMissedStageCount(stream, readyUpload.getRollTs());
                        LOG.info("need recovery missed stages: " + missedStageCount);
                        for (Stage stage : stages) {
                            stage.setCurrent(false);
                        }
                        Issue issue = new Issue();
                        issue.setTs(stream.getLastSuccessTs() + stream.getPeriod() * 1000);
                        issue.setDesc("log discontinuous");
                        ArrayList<Stage> missedStages = getMissedStages(stream, missedStageCount, issue);
                        for (Stage missedStage : missedStages) {
                            LOG.info("processing missed stages: " + missedStage);
                            // check whether it is missed
                            if (missedStage.isCurrent()) {
                                missedStage.setCurrent(false);
                                current = missedStage;
                            }
                            if (!stages.contains(missedStage)) {
                                stages.add(missedStage);
                            }
                            doRecovery(stream, missedStage);
                        }
                    } else {
                        if (current.isCleanstart() == false || current.getIssuelist().size() != 0) {
                            doRecovery(stream, current);
                        } else {
                            current.setStatus(Stage.UPLOADING);
                            doUpload(stream, current, from);
                        }
                    }
                    // create next stage if stream is connected
                    if (current.getStatus() != Stage.PENDING && current.getStatus() != Stage.BROKERFAIL) {
                        current.setCurrent(false);
                        Stage next = new Stage();
                        next.setTopic(stream.getTopic());
                        next.setSource(stream.getSource());
                        next.setBrokerHost(current.getBrokerHost());
                        next.setCleanstart(true);
                        next.setRollTs(current.getRollTs() + stream.getPeriod() * 1000);
                        next.setStatus(Stage.APPENDING);
                        next.setIssuelist(new Vector<Issue>());
                        next.setCurrent(true);
                        stages.add(next);
                    }
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        } else {
            LOG.error("can't find stream by " + readyUpload.getTopic() + ":" + readyUpload.getSource());
        }
    }
    
    private String doUpload(Stream stream, Stage current, ByteBufferNonblockingConnection from) {
        return doUpload(stream, current, from, false);
    }
    
    private String doUpload(Stream stream, Stage current, ByteBufferNonblockingConnection from, boolean isFinal) {
        TopicConfig config = configManager.getConfByTopic(stream.getTopic());
        String compression = config.getCompression();
        boolean persistent = config.isPersistent();
        Message message = PBwrap.wrapUploadRoll(
                current.getTopic(),
                current.getSource(),
                stream.getPeriod(),
                current.getRollTs(),
                isFinal,
                persistent,
                compression
        );
        send(from, message);
        stageConnectionMap.put(current, from);
        return from.getHost();
    }
    
    public void doRecovery(Stream stream, Stage stage) {
        doRecovery(stream, stage, false);
    }
    
    /*
     * do recovery of one stage of a stream
     * if the stream is not active or no broker now,
     * mark the stream as pending
     * else send recovery message
     */
    private void doRecovery(Stream stream, Stage stage, boolean isFinal) {
        String broker = null;
        broker = getBrokerRandom();

        if (broker == null || !stream.isActive()) {
            stage.setStatus(Stage.PENDING);
            stageConnectionMap.remove(stage);
        } else {
            TopicConfig config = configManager.getConfByTopic(stream.getTopic());
            boolean persistent = config.isPersistent();
            Message message = PBwrap.wrapRecoveryRoll(
                    stream.getTopic(),
                    broker,
                    getRecoveryPort(broker),
                    stage.getRollTs(),
                    stream.getSource(),
                    isFinal,
                    persistent
                    );
            stage.setStatus(Stage.RECOVERYING);
            stage.setBrokerHost(broker);
            ByteBufferNonblockingConnection c = dataSourceMapping.get(Util.getHostFromSource(stream.getSource()));
            if (c != null) {
                send(c, message);
            } else {
                LOG.error("can not find connection by host: " + stream.getSource());
            }
            ByteBufferNonblockingConnection brokerConnection = brokersMapping.get(broker);
            if (brokerConnection != null) {
                stageConnectionMap.put(stage, brokerConnection);
            }
        }
    }
    
    /*
     * record the stream if it is a new stream;
     * do recovery if it is a old stream
     */
    private void handleReadyStream(ReadyStream readyBroker, ByteBufferNonblockingConnection from) {
        long connectedTs = readyBroker.getConnectedTs();
        long currentTs = Util.getCurrentRollTs(connectedTs, readyBroker.getPeriod());
        String brokerHost = readyBroker.getBrokerServer();
        
        String topic = readyBroker.getTopic();
        Topic t = topics.get(topic);
        if (t == null) {
            t = new Topic(topic);
            topics.put(topic, t);
            LOG.info("new topic added: " + t);
        }
        //source is an alias of partitionId in "Batch Mode"
        String source = readyBroker.getPartitionId();
        Stream stream = getStream(readyBroker.getTopic(), source);
        // processing stream affairs
        if (stream == null) {
            // record new stream
            stream = new Stream(readyBroker.getTopic(), source);
            stream.setBrokerHost(readyBroker.getBrokerServer());
            stream.setStartTs(connectedTs);
            stream.setPeriod(readyBroker.getPeriod());
            long initLastSuccessTs = currentTs - stream.getPeriod() * 1000;
            long storedLastSuccessTs = checkpoint.getStoredLastSuccessTs(stream);
            LOG.debug("init ts " + initLastSuccessTs + " stored ts " + storedLastSuccessTs);
            boolean shouldRecovery = false;
            if (storedLastSuccessTs != initLastSuccessTs && storedLastSuccessTs != 0) {
                stream.updateLastSuccessTs(storedLastSuccessTs);
                shouldRecovery = true;
            } else {
                stream.updateLastSuccessTs(initLastSuccessTs);
            }
            ArrayList<Stage> stages = new ArrayList<Stage>();
            Stage current = new Stage();
            current.setTopic(stream.getTopic());
            current.setSource(stream.getSource());
            current.setBrokerHost(readyBroker.getBrokerServer());
            current.setCleanstart(false);
            current.setIssuelist(new Vector<Issue>());
            current.setStatus(Stage.APPENDING);
            current.setRollTs(currentTs);
            current.setCurrent(true);
            stages.add(current);
            stream.setStages(stages);
            addStream(stream);
            if (shouldRecovery) {
                synchronized (stages) {
                    int missedStageCount = getMissedStageCount(stream, initLastSuccessTs);
                    LOG.info("need recovery possible missed stages: " + missedStageCount);
                    Issue issue = new Issue();
                    issue.setTs(connectedTs);
                    issue.setDesc("stream reconnected but find should recovery");
                    ArrayList<Stage> missedStages = getMissedStages(stream, missedStageCount, issue);
                    for (Stage missedStage : missedStages) {
                        LOG.info("processing missed stages: " + missedStage);
                        // check whether it is missed
                        if (!stages.contains(missedStage)) {
                            stages.add(missedStage);
                        }
                        doRecovery(stream, missedStage);
                    }
                }
            } else {
                LOG.info("no need to recovery any stage.");
            }
        } else {
            // old stream reconnected
            LOG.info("stream reconnected: " + stream);
            if (!stream.isActive()) {
                stream.updateActive(true);
            }
            
            // update broker on stream
            stream.setBrokerHost(readyBroker.getBrokerServer());

            List<Stage> currentStages = stream.getStages();
            if (currentStages != null) {
                synchronized (currentStages) {
                    recoveryStages(stream, currentStages, connectedTs, currentTs, readyBroker.getBrokerServer());
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        }
        
        // register connection with agent and broker
        recordConnectionStreamMapping(from, stream);
        //source may be a KVM agent, or a PaaS instance, or a producerId
        //host may be a KVM agent host, or a PaaS physics machine, or a Storm worker host
        String host = Util.getHostFromSource(source);
        ByteBufferNonblockingConnection dataSourceConnection = dataSourceMapping.get(host);
        if (dataSourceConnection == null) {
            LOG.fatal("Oops, can not get Connection by " + host);
        } else {
            recordConnectionStreamMapping(dataSourceConnection, stream);
        }
        
        // process partition affairs
        String partitionId = readyBroker.getPartitionId();
        
        // online partitionId in producer manager
        ProducerManager producerManager = producerMgrMap.get(topic);
        if (producerManager != null) {
            producerManager.switchToOnline(partitionId);
        }
        
        PartitionInfo pinfo = t.getPartition(partitionId);
        if (pinfo == null) {
            pinfo = new PartitionInfo(partitionId, readyBroker.getBrokerServer());
            LOG.info("new partition online: " + pinfo);
            t.addPartition(partitionId, pinfo);
        } else {
            if (pinfo.isOffline()) {
                pinfo.updateHost(brokerHost);
                pinfo.markOffline(false);
                LOG.info("partition back to online: " + pinfo);
            }
        }
        //delete LogNotFound entry if exists
        configManager.removeLogNotFound(topic, host);
        
        //reassign consumer
        reassignConsumers(topic);
    }

    private void reassignConsumers(String topic) {
        for (ConsumerGroup group : consumerGroups.values()) {
            if (!topic.equals(group.getTopic())) {
                continue;
            }
            LOG.info("reassign consumer of topic: " + topic);
            tryAssignConsumer(null, group);
        }
    }

    /*
     * 1. recovery appFail or broker fail stages (include current stage, but do no recovery)
     * 2. recovery missed stages (include current stage, but do no recovery)
     * 3. recovery current stage (realtime stream)with broker fail
     */
    private void recoveryStages(Stream stream, List<Stage> currentStages, long connectedTs, long currentTs, String newBrokerHost) {
        Issue issue = new Issue();
        issue.setTs(connectedTs);
        issue.setDesc("stream reconnected");
        
        // recovery Pending and BROKERFAIL stages
        for (int i = 0 ; i < currentStages.size(); i++) {
            Stage stage = currentStages.get(i);
            stage.setCurrent(false);
            if (!stage.getIssuelist().contains(issue)) {
                stage.getIssuelist().add(issue);
            }
            if (stage.getStatus() == Stage.PENDING
                    || stage.getStatus() == Stage.BROKERFAIL
                    || stage.getStatus() == Stage.PAUSE) {
                // do not recovery current stage
                if (stage.getRollTs() != currentTs) {
                    LOG.info("processing pending stage: " + stage);
                    doRecovery(stream, stage);
                } else {
                    // fix current stage status
                    stage.setCurrent(true);
                    stage.setStatus(Stage.APPENDING);
                    stage.setBrokerHost(newBrokerHost);
                }
            }
        }

        // recovery missed stages
        int missedStageCount = getMissedStageCount(stream, connectedTs);
        LOG.info("need recovery possible missed stages: " + missedStageCount);
        
        ArrayList<Stage> missedStages = getMissedStages(stream, missedStageCount, issue);
        for (Stage stage : missedStages) {
            // check whether it is missed
            if (!currentStages.contains(stage)) {
                LOG.info("process missed stage: " + stage);
                // do not recovery current stage
                if (stage.getRollTs() != currentTs) {
                    doRecovery(stream, stage);
                } else {
                    stage.setCurrent(true);
                    stage.setStatus(Stage.APPENDING);
                    stage.setBrokerHost(newBrokerHost);
                }
                currentStages.add(stage);
            }
        }
    }
   
    private void recordConnectionStreamMapping(ByteBufferNonblockingConnection connection, Stream stream) {
        connectionStreamMap.putIfAbsent(connection, new ArrayList<Stream>());
        ArrayList<Stream> streams = connectionStreamMap.get(connection);
        synchronized (streams) {
            if (!streams.contains(stream)) {
                streams.add(stream);
            }
        }
    }
    
    private int getMissedStageCount(Stream stream, long connectedTs) {
        long rollts = Util.getCurrentRollTs(connectedTs, stream.getPeriod());
        return (int) ((rollts - stream.getLastSuccessTs()) / stream.getPeriod() / 1000);
    }
    
    /*
     * caller must hold the monitor of stages
     */
    private ArrayList<Stage> getMissedStages(Stream stream, int missedStageCount, Issue issue) {
        ArrayList<Stage> missedStages = new ArrayList<Stage>();
        for (int i = 0; i< missedStageCount; i++) {
            Stage stage = new Stage();
            stage.setTopic(stream.getTopic());
            stage.setSource(stream.getSource());
            if (i == missedStageCount-1) {
                stage.setCurrent(true);
            } else {
                stage.setCurrent(false);
            }
            stage.setIssuelist(new Vector<Issue>());
            stage.getIssuelist().add(issue);
            stage.setStatus(Stage.RECOVERYING);
            stage.setRollTs(stream.getLastSuccessTs() + stream.getPeriod() * 1000 * (i+1));
            missedStages.add(stage);
        }
        return missedStages;
    }
    
    /*
     * 1. recored the connection in agent
     * 2. assign a broker to the topic
     */
    private void handleTopicReg(AppReg appReg, ByteBufferNonblockingConnection from) {
        if (!from.isResolved()) {
            handleUnresolvedConnection(from);
            return;
        }
        ConnectionDesc desc = connections.get(from);
        if (desc == null) {
            LOG.error("can not find ConnectionDesc by connection " + from);
            return;
        }
        desc.setType(ConnectionDesc.AGENT);
        String source = appReg.getSource();
        if(null == dataSourceMapping.putIfAbsent(from.getHost(), from)) {
            LOG.info("Topic " + appReg.getTopic() + " registered from " + source);
        }
        assignBroker(appReg.getTopic(), from, source, source);
    }

    private String assignBroker(String topic, ByteBufferNonblockingConnection from, String source, String partitionId) {
        String instanceId = Util.getInstanceIdFromSource(source);   //null if source is KVM or ProducerId
        String broker = getBroker(topic, partitionId);
        Message message;
        if (broker != null) {
            message = PBwrap.wrapAssignBroker(PBwrap.assignBroker(topic, broker, getBrokerPort(broker), instanceId, partitionId));
        } else {
            message = PBwrap.wrapNoAvailableNode(topic, instanceId, source);
        }
        LOG.info("assign broker: " + broker + " for topic: " + topic
                + " source/producer: " + source
                + (partitionId == null ? "" : (" partition: " + partitionId)));
        send(from, message);
        return broker;
    }

    private void handleBrokerReg(BrokerReg brokerReg, ByteBufferNonblockingConnection from) {
        if (!from.isResolved()) {
            handleUnresolvedConnection(from);
            return;
        }
        ConnectionDesc desc = connections.get(from);
        if (desc == null) {
            LOG.error("can not find ConnectionDesc by connection " + from);
            return;
        }
        desc.setType(ConnectionDesc.BROKER);
        desc.attach(new BrokerDesc(from.toString(), brokerReg.getBrokerPort(), brokerReg.getRecoveryPort()));
        brokersMapping.put(from.getHost(), from);
        LOG.info("Broker " + from.getHost() + " registered");
    }

    /* 
     * get broker by pre-assignment
     * or return a broker randomly
     * if no brokers now, return null
     */
    private String getBroker(String topic, String partition) {
        if ((configManager.brokerAssignmentLimitEnable)
                && (brokersMapping.size() < configManager.brokerAssignmentLimitMin)) {
            LOG.info("Can not get any broker! Alive broker number "
                    + brokersMapping.size() + " < limit min "
                    + configManager.brokerAssignmentLimitMin);
            return null;
        }

        String broker = configManager.getBrokerPreassignmentByPartition(topic, partition);
        if (broker != null) {
            if (brokersMapping.containsKey(broker)) {
                LOG.info(topic + "@" + partition
                        + " get the preassignment broker " + broker);
                return broker;
            } else {
                LOG.info(topic + "@" + partition
                        + " can not get the preassignment broker " + broker
                        + " because of absence, get anther randomly");
                return getBrokerRandom();
            }
        } else {
            return getBrokerRandom();
        }
    }

    private String getBrokerRandom() {
        String broker = null;
        ArrayList<String> array = new ArrayList<String>(brokersMapping.keySet());
        if (array.size() != 0) {
            Random random = new Random();
            broker = array.get(random.nextInt(array.size()));
        }
        return broker;
    }
    
    private void handleRollingAndTriggerClean(RollClean rollClean, ByteBufferNonblockingConnection from) {
        boolean isFinal = true;
        Stream stream = getStream(rollClean.getTopic(), rollClean.getSource());
        if (stream != null) {
            List<Stage> stages = stream.getStages();
            if (stages != null && stages.size() != 0) {
                synchronized (stages) {
                    Stage current = stages.get(stages.size() - 1);
                    if (current.isCleanstart() == false || current.getIssuelist().size() != 0) {
                        current.setCurrent(false);
                        doRecovery(stream, current, isFinal);
                    } else {
                        if (current.getStatus() != Stage.UPLOADING) {
                            current.setStatus(Stage.UPLOADING);
                            current.setCurrent(false);
                            doUpload(stream, current, from, isFinal);
                        }
                    }
                }
            } else {
                LOG.info("no stages of stream: " + stream + ", clean it up.");
            }
        } else {
            LOG.info("stream " + rollClean.getTopic() + ":" + rollClean.getSource() + " has been clean up.");
        }
    }
    
    private void handleProducerReg(ProducerReg producerReg, ByteBufferNonblockingConnection from) {
        ConnectionDesc desc = connections.get(from);
        if (desc == null) {
            LOG.error("can not find ConnectionDesc by connection " + from);
            return;
        }
        desc.setType(ConnectionDesc.PRODUCER);
        
        String topic = producerReg.getTopic();
        String producerId = producerReg.getProducerId();
        
        ProducerDesc producerDesc = new ProducerDesc(producerId, topic);
        desc.attach(producerDesc);
        if(null == dataSourceMapping.putIfAbsent(producerId, from)) {
            LOG.info("Topic " + topic + " registered from " + producerId);
        }
        assignPartition(topic, producerId, from);
    }

    private void assignPartition(String topic, String producerId, ByteBufferNonblockingConnection from) {
        Message message = null;
        ProducerManager producerMgr = producerMgrMap.get(topic);
        if (producerMgr == null) {
            LOG.error("Oops, can not find producer manager by " + topic);
            return;
        }
        List<String> partitionIds = producerMgr.generatePartitionId(producerId);
        int partitionFactor = producerMgr.getPartitionFactor();
        List<AssignBroker> assigns = new ArrayList<AssignBroker>(partitionFactor);
        if (partitionIds.isEmpty()) {
            LOG.error("Oops! can not get any partitions for " + topic + " " + producerId);
            return;
        }
        for (String partitionId : partitionIds) {
            String broker = getBroker(topic, partitionId);
            if (broker == null) {
                message = PBwrap.wrapNoAvailableNode(topic, null, producerId);
            } else {
                AssignBroker assignBroker = PBwrap.assignBroker(topic, broker, getBrokerPort(broker), null, partitionId);
                assigns.add(assignBroker);
                LOG.info("assign parition "  + partitionId + " with " + broker + " for " + topic);
                message = PBwrap.wrapAssignParitions(assigns);
            }
        }
        send(from, message);
    }
    

    private void handlePartitionRequireBroker(
            PartitionRequireBroker partitionRequireBroker,
            ByteBufferNonblockingConnection from) {
        String topic = partitionRequireBroker.getTopic();
        String produceId = partitionRequireBroker.getProducerId();
        String partitionId = partitionRequireBroker.getPartitionId();
        assignBroker(topic, from, produceId, partitionId);
    }

    public void handleUnresolvedConnection(ByteBufferNonblockingConnection from) {
        LOG.warn("Found an unresolved connection [" + from.getHost() + "<->" + from.getIP() + "]");
        Message message = PBwrap.wrapUnresolvedConnection();
        send(from, message);
    }
    
    public class SupervisorExecutor implements EntityProcessor<ByteBuffer, ByteBufferNonblockingConnection> {

        @Override
        public void OnConnected(ByteBufferNonblockingConnection connection) {
            ConnectionDesc desc = connections.get(connection);
            if (desc == null) {
                LOG.info("client " + connection + " connected");
                synchronized (connections) {
                    connections.put(connection, new ConnectionDesc(connection));
                }
                //trigger PAAS config response
                triggerConfResOfPaaS(connection);
            } else {
                LOG.error("connection already registered: " + connection);
            }
        }

        @Override
        public void OnDisconnected(ByteBufferNonblockingConnection connection) {
            closeConnection(connection);
        }
        
        @Override
        public void process(ByteBuffer request, ByteBufferNonblockingConnection from) {
            Message msg = null;
            try {
                msg = PBwrap.Buf2PB(request);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("InvalidProtocolBufferException catched ", e);
                return;
            }
            
            switch (msg.getType()) {
            case HEARTBEART:
                handleHeartBeat(msg, from);
                break;
            case BROKER_REG:
                LOG.debug("received: " + msg);
                handleBrokerReg(msg.getBrokerReg(), from);
                break;
            case APP_REG:
                LOG.debug("received: " + msg);
                handleTopicReg(msg.getAppReg(), from);
                break;
            case CONSUMER_REG:
                LOG.debug("received: " + msg);
                handleConsumerReg(msg.getConsumerReg(), from);
                break;
            case READY_STREAM:
                LOG.debug("received: " + msg);
                handleReadyStream(msg.getReadyStream(), from);
                break;
            case READY_UPLOAD:
                LOG.debug("received: " + msg);
                handleRolling(msg.getReadyUpload(), from);
                break;
            case UPLOAD_SUCCESS:
                LOG.debug("received: " + msg);
                handleUploadSuccess(msg.getRollID(), from);
                break;
            case UPLOAD_FAIL:
                LOG.debug("received: " + msg);
                handleUploadFail(msg.getRollID());
                break;
            case RECOVERY_SUCCESS:
                LOG.debug("received: " + msg);
                handleRecoverySuccess(msg.getRollID());
                break;
            case RECOVERY_FAIL:
                LOG.debug("received: " + msg);
                handleRecoveryFail(msg.getRollID());
                break;
            case FAILURE:
                LOG.debug("received: " + msg);
                handleFailure(msg.getFailure());
                break;
            case UNRECOVERABLE:
                LOG.debug("received: " + msg);
                handleUnrecoverable(msg.getRollID());
                break;
            case TOPIC_REPORT:
                handleTopicReport(msg.getTopicReport(), from);
                break;
            case OFFSET_COMMIT:
                handleOffsetCommit(msg.getOffsetCommit());
                break;
            case MANUAL_RECOVERY_ROLL:
                handleManualRecoveryRoll(msg.getRollID());
                break;
            case DUMP_STAT:
                dumpstat(from);
                break;
            case RETIRE_STREAM:
                handleRetireStream(msg.getRetire(), from);
                break;
            case CONF_REQ:
                handleConfReq(msg.getConfReq(), from);
                break;
            case DUMP_CONF:
                dumpconf(from);
                break;
            case LIST_APPS:
                listTopics(from);
                break;
            case REMOVE_CONF:
                removeConf(msg.getRemoveConf(), from);
                break;
            case DUMP_APP:
                dumpTopic(msg.getDumpApp(), from);
                break;
            case LIST_IDLE:
                listIdle(from);
                break;
            case RESTART:
                handleRestart(msg.getRestart());
                break;
            case ROLL_CLEAN:
                handleRollingAndTriggerClean(msg.getRollClean(), from);
                break;
            case DUMP_CONSUMER_GROUP:
                dumpConsumerGroup(msg.getDumpConsumerGroup(), from);
                break;
            case LIST_CONSUMER_GROUP:
                listConsumerGroups(from);
                break;
            case PRODUCER_REG:
                handleProducerReg(msg.getProducerReg(), from);
                break;
            case PARTITION_REQUIRE_BROKER:
                handlePartitionRequireBroker(msg.getPartitionRequireBroker(), from);
                break;
            case CONSUMER_EXIT:
                handleConsumerExit(msg.getConsumerExit(), from);
                break;
            case LOG_NOT_FOUND:
                handleLogNotFound(msg.getLogNotFound(), from);
                break;
            default:
                LOG.warn("unknown message: " + msg.toString());
            }
        }
    }
    
    private class LiveChecker extends Thread {
        private static final int LIVE_CHECK_INTERVAL = 10000;
        private static final int THRESHOLD = 60000;
        boolean running = true;
        
        public LiveChecker() {
            this.setDaemon(true);
            this.setName("LiveChecker");
        }
        
        @Override
        public void run() {
            while (running) {
                try {
                    Thread.sleep(LIVE_CHECK_INTERVAL);
                    for (Entry<ByteBufferNonblockingConnection, ConnectionDesc> entry : connections.entrySet()) {
                        ConnectionDesc dsc = entry.getValue();
                        if (dsc.getType() != ConnectionDesc.AGENT &&
                            dsc.getType() != ConnectionDesc.BROKER &&
                            dsc.getType() != ConnectionDesc.CONSUMER &&
                            dsc.getType() != ConnectionDesc.PRODUCER) {
                            continue;
                        }
                        ByteBufferNonblockingConnection conn = entry.getKey();
                        long now = Util.getTS();
                        if (now - dsc.getLastHeartBeat() > THRESHOLD) {
                            LOG.info("failed to get heartbeat for 60 seconds, "
                                    + " last receive heartbeat ts is " + dsc.getLastHeartBeat()
                                    + ", close connection " + conn);
                            server.closeConnection(conn);
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.info("LiveChecker thread interrupted");
                    running = false;
                } catch (Throwable t) {
                    LOG.error("Oops, catch an exception in LiveChecker, but go on.", t);
                }
            }
        }
    }
       
    private void init() throws IOException, LionException {
        topics = new ConcurrentHashMap<String, Topic>();
        connectionStreamMap = new ConcurrentHashMap<ByteBufferNonblockingConnection, ArrayList<Stream>>();
        stageConnectionMap = new ConcurrentHashMap<Stage, ByteBufferNonblockingConnection>();

        consumerGroups = new ConcurrentHashMap<ConsumerGroupKey, ConsumerGroup>();
        
        connections = new ConcurrentHashMap<ByteBufferNonblockingConnection, ConnectionDesc>();
        dataSourceMapping = new ConcurrentHashMap<String, ByteBufferNonblockingConnection>();
        brokersMapping = new ConcurrentHashMap<String, ByteBufferNonblockingConnection>();
        producerMgrMap = new ConcurrentHashMap<String, ProducerManager>();
        
        //initConfManager(or lion/zookeeper)
        configManager = new ConfigManager(this);
        configManager.initConfig();
        
        //initWebService
        RequestListener httpService = new RequestListener(configManager);
        httpService.setDaemon(true);
        httpService.start();
        
        //start restful server
        ServiceFactory.setConfigManger(configManager);
        ServiceFactory.setSupervisor(this);
        String restfulServerAddr = "http://" + Util.getLocalHostIP() + ":" + Integer.toString(configManager.jettyPort);
        HttpServer restfulServer = new HttpServer.Builder().setName("supervisor").addEndpoint(URI.create(restfulServerAddr)).build();
        restfulServer.addJerseyResourcePackage(ServiceFactory.class.getPackage().getName(), "/*");
        restfulServer.start();
        
        // start heart beat checker thread
        LiveChecker checker = new LiveChecker();
        checker.start();
        
        checkpoint = new Checkpoint(configManager.checkpiontPath, configManager.checkpiontPeriod);
        checkpoint.start();
        
        AbnormalStageChecker abnormalStageChecker = new AbnormalStageChecker(configManager.abnormalStageCheckPeriod, configManager.abnormalStageDuration, configManager.normalStageTTL);
        abnormalStageChecker.start();
        
        SupervisorExecutor executor = new SupervisorExecutor();
        NonblockingConnectionFactory<ByteBufferNonblockingConnection> factory = new ByteBufferNonblockingConnection.ByteBufferNonblockingConnectionFactory();
        server = new GenServer<ByteBuffer, ByteBufferNonblockingConnection, EntityProcessor<ByteBuffer, ByteBufferNonblockingConnection>>
            (executor, factory, null);

        server.init("supervisor", configManager.supervisorPort, configManager.numHandler);
    }

    public void handleLogNotFound(LogNotFound logNotFound, ByteBufferNonblockingConnection from) {
        String instanceId = logNotFound.getInstanceId();
        String host = from.getHost();
        String source = Util.getSource(host, instanceId);
        configManager.addLogNotFound(logNotFound.getTopic(), host, logNotFound.getFile(), logNotFound.getTs(), source);
    }

    public void handleConsumerExit(ConsumerExit consumerExit,
            ByteBufferNonblockingConnection from) {
        ConnectionDesc desc = connections.get(from);
        if (desc == null) {
            LOG.error("can not find ConnectionDesc by connection " + from);
            return;
        }
        String groupId = consumerExit.getGroupId();
        String consumerId = consumerExit.getConsumerId();
        String topic = consumerExit.getTopic();
        
        ConsumerGroupKey groupKey = new ConsumerGroupKey(groupId, topic);
        ConsumerGroup group = consumerGroups.get(groupKey);
        if (group == null) {
            LOG.warn("Can not find group by " + groupKey);
            return;
        }
        ConsumerDesc consumerDesc = new ConsumerDesc(consumerId, groupId, topic, from);
        desc.detach(consumerDesc);
        
        group.unregisterConsumer(consumerDesc);
        
        if (group.getConsumerCount() == 0) {
            LOG.info("consumerGroup " + groupKey +" has not live consumer, thus be removed");
            consumerGroups.remove(groupKey);
        } else {
            // consumer user thread maybe not work, reassign to avoid data loss
            LOG.info("handle consumer exit " + consumerDesc + ", try re-assign consumer");
            tryAssignConsumer(null, group);
        }
    }

    public void handleConfReq(ConfReq confReq, ByteBufferNonblockingConnection from) {
        String topicFromReq = confReq.getTopic();
        if (topicFromReq != null && topicFromReq.length() != 0) {
            //producer request
            triggerConfResOfProducer(from, topicFromReq);
        } else {
            //try handle paas request first
            triggerConfResOfPaaS(from);
            //then handle kvm request
            triggerConfResOfKvm(from);
        }
    }

    private void triggerConfResOfProducer(ByteBufferNonblockingConnection from,
            String topicFromReq) {
        Message message;
        TopicConfig topicConfig = configManager.getConfByTopic(topicFromReq);
        if (topicConfig == null) {
            message = PBwrap.wrapNoAvailableConf(topicFromReq);
        } else {
            int partitionFactor = topicConfig.getPartitionFactor();
            producerMgrMap.putIfAbsent(topicFromReq, new ProducerManager(topicFromReq, partitionFactor));
            ProducerManager manager = producerMgrMap.get(topicFromReq);
            String producerId = manager.getProducerId();
            
            CommonConfRes commonConfRes = PBwrap.wrapCommonConfRes(
                    topicConfig.getMaxLineSize(),
                    topicConfig.getMinMsgSent(),
                    topicConfig.getMsgBufSize(),
                    topicConfig.getRollPeriod(),
                    topicConfig.getPartitionFactor());
            message = PBwrap.wrapProducerIdAssign(topicFromReq, producerId, commonConfRes);
        }
        send(from, message);
    }

    private void triggerConfResOfKvm(ByteBufferNonblockingConnection from) {
        Message message;
        List<AppConfRes> appConfResList = new ArrayList<AppConfRes>();
        Set<String> topicsAssocHost = configManager.getTopicsByHost(from.getHost());
        if (topicsAssocHost == null || topicsAssocHost.size() == 0) {
            LOG.debug("No topic mapping to " + from.getHost());
            message = PBwrap.wrapNoAvailableConf(null);
            send(from, message);
            return;
        }
        for (String topic : topicsAssocHost) {
            TopicConfig confInfo = configManager.getConfByTopic(topic);
            if (confInfo == null) {
                LOG.error("Can not get topic: " + topic + " from configMap");
                continue;
            }
            String rotatePeriod = String.valueOf(confInfo.getRotatePeriod());
            String rollPeriod = String.valueOf(confInfo.getRollPeriod());
            String maxLineSize = String.valueOf(confInfo.getMaxLineSize());
            String readInterval = String.valueOf(confInfo.getReadInterval());
            String minMsgSent = String.valueOf(confInfo.getMinMsgSent());
            String msgBufSize = String.valueOf(confInfo.getMsgBufSize());
            String bandwidthPerSec = String.valueOf(confInfo.getBandwidthPerSec());
            long tailPosition = confInfo.getTailPosition();
            String watchFile = confInfo.getWatchLog();
            if (watchFile == null) {
                LOG.error("Can not get watch file of " + topic);
                continue;
            }
            AppConfRes appConfRes = PBwrap.wrapAppConfRes(topic, watchFile,
                    rotatePeriod, rollPeriod, maxLineSize, readInterval,
                    minMsgSent, msgBufSize, bandwidthPerSec, tailPosition);
            appConfResList.add(appConfRes);
        }
        if (appConfResList.isEmpty()) {
            message = PBwrap.wrapNoAvailableConf(null);
            send(from, message);
        } else {
            message = PBwrap.wrapConfRes(appConfResList, null);
            send(from, message);
        }
    }
    
    //TODO find update of paas catalog
    public void triggerConfResOfPaaS(ByteBufferNonblockingConnection connection) {
        Set<String> topicsAssocHost = configManager.getTopicsByHost(connection.getHost());
        if (topicsAssocHost != null) {
            List<LxcConfRes> lxcConfResList = new ArrayList<LxcConfRes>();
            for (String topic : topicsAssocHost) {
                TopicConfig confInfo = configManager.getConfByTopic(topic);
                if (confInfo == null) {
                    LOG.error("Can not get topic: " + topic + " from configMap");
                    continue;
                }
                Set<String> ids = confInfo.getInsByHost(connection.getHost());
                if (ids == null) {
                    LOG.info("Can not get instances by " + topic + " and " + connection.getHost());
                    continue;
                }
                String rotatePeriod = String.valueOf(confInfo.getRotatePeriod());
                String rollPeriod = String.valueOf(confInfo.getRollPeriod());
                String maxLineSize = String.valueOf(confInfo.getMaxLineSize());
                String readInterval = String.valueOf(confInfo.getReadInterval());
                String watchFile = confInfo.getWatchLog();
                String minMsgSent = String.valueOf(confInfo.getMinMsgSent());
                String msgBufSize = String.valueOf(confInfo.getMsgBufSize());
                String bandwidthPerSec = String.valueOf(confInfo.getBandwidthPerSec());
                long tailPosition = confInfo.getTailPosition();
                LxcConfRes lxcConfRes = PBwrap.wrapLxcConfRes(topic, watchFile,
                        rotatePeriod, rollPeriod, maxLineSize, readInterval,
                        minMsgSent, msgBufSize, bandwidthPerSec, tailPosition, ids);
                lxcConfResList.add(lxcConfRes);
            }
            if (!lxcConfResList.isEmpty()) {
                Message message = PBwrap.wrapConfRes(null, lxcConfResList);
                send(connection, message);
            }
        } else {
            LOG.debug("No topic mapping to " + connection.getHost());
        }
    }
        
    private int getBrokerPort(String host) {
        ByteBufferNonblockingConnection connection = brokersMapping.get(host);
        if (connection == null) {
            LOG.error("can not get connection from host: " + host);
            return 0;
        }
        ConnectionDesc desc = connections.get(connection);
        if (desc == null) {
            LOG.error("can not get ConnectionDescription from connection: " + connection);
            return 0;
        }
        List<NodeDesc> nodeDescs = desc.getAttachments();
        BrokerDesc brokerDesc = (BrokerDesc) nodeDescs.get(0);
        return brokerDesc.getBrokerPort();
    }
    
    private int getRecoveryPort(String host) {
        ByteBufferNonblockingConnection connection = brokersMapping.get(host);
        if (connection == null) {
            LOG.error("can not get connection from host: " + host);
            return 0;
        }
        ConnectionDesc desc = connections.get(connection);
        if (desc == null) {
            LOG.error("can not get ConnectionDescription from connection: " + connection);
            return 0;
        }
        List<NodeDesc> nodeDescs = desc.getAttachments();
        BrokerDesc brokerDesc = (BrokerDesc) nodeDescs.get(0);
        return brokerDesc.getRecoveryPort();
    }
    
    /**
     * Attention, this method is not fast, it will traverse all map elements
     * @param hostname
     * @return
     */
    public ByteBufferNonblockingConnection getIdleConnectionByHostname(String hostname) {
        ByteBufferNonblockingConnection conn;
        conn = getDataSourceConnectionByHostname(hostname);
        if (conn != null) {
            return null;
        }
        synchronized (connections) {
            for (Map.Entry<ByteBufferNonblockingConnection, ConnectionDesc> connectionEntry : connections.entrySet()) {
                if (connectionEntry.getKey().getHost().equals(hostname)
                        && connectionEntry.getValue().getType() != ConnectionDesc.AGENT
                        && connectionEntry.getValue().getType() != ConnectionDesc.BROKER
                        && connectionEntry.getValue().getType() != ConnectionDesc.CONSUMER
                        && connectionEntry.getValue().getType() != ConnectionDesc.PRODUCER) {
                    return connectionEntry.getKey();
                }
            }
        }
        return null;
    }
    
    public ByteBufferNonblockingConnection getDataSourceConnectionByHostname(String hostname) {
        return dataSourceMapping.get(hostname);
    }
    
    public ByteBufferNonblockingConnection getBrokerConnectionByHostname(String hostname) {
        return brokersMapping.get(hostname);
    }
    
    public ConnectionInfo getConnectionInfoByHostname(String hostname) {
        ConnectionInfo connInfo = null;
        ByteBufferNonblockingConnection connection = getDataSourceConnectionByHostname(hostname);
        if (connection != null) {
            ConnectionDesc connDesc = connections.get(connection);
            if (connDesc != null) {
                connInfo = connDesc.getConnectionInfo();
            }
        }
        return connInfo;
    }
    
    public int getConnectionType(ByteBufferNonblockingConnection connection) {
        ConnectionDesc des = connections.get(connection);
        if (des == null) {
            return 0;
        }
        return des.getType();
    }
    
    /**
     * @param args
     * @throws IOException 
     * @throws LionException
     */
    public static void main(String[] args) throws IOException, LionException {
        Supervisor supervisor = new Supervisor();
        supervisor.init();
    }
}

package com.dp.blackhole.common;

import java.util.ArrayList;
import java.util.List;

import com.dp.blackhole.collectornode.persistent.PersistentManager.reporter.reportEntry;
import com.dp.blackhole.common.gen.AppRegPB.AppReg;
import com.dp.blackhole.common.gen.AppRollPB.AppRoll;
import com.dp.blackhole.common.gen.AssignCollectorPB.AssignCollector;
import com.dp.blackhole.common.gen.AssignConsumerPB.AssignConsumer;
import com.dp.blackhole.common.gen.ConsumerRegPB.ConsumerReg;
import com.dp.blackhole.common.gen.FailurePB.Failure;
import com.dp.blackhole.common.gen.FailurePB.Failure.NodeType;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.MessagePB.Message.MessageType;
import com.dp.blackhole.common.gen.NoAvailableNodePB.NoAvailableNode;
import com.dp.blackhole.common.gen.OffsetCommitPB.OffsetCommit;
import com.dp.blackhole.common.gen.ReadyCollectorPB.ReadyCollector;
import com.dp.blackhole.common.gen.RecoveryRollPB.RecoveryRoll;
import com.dp.blackhole.common.gen.RollIDPB.RollID;
import com.dp.blackhole.common.gen.StreamIDPB.StreamID;
import com.dp.blackhole.common.gen.TopicReportPB.TopicReport;
import com.dp.blackhole.supervisor.PartitionInfo;

public class PBwrap {
    public static Message wrapMessage(MessageType type, Object message) {
        Message.Builder msg = Message.newBuilder();
        msg.setType(type);
        switch (type) {
        case NOAVAILABLENODE:
            msg.setNoAvailableNode((NoAvailableNode) message);
            break;
        case HEARTBEART:
            break;
        case TOPICREPORT:
            msg.setTopicReport((TopicReport) message);
            break;
        case APP_REG:
            msg.setAppReg((AppReg) message);
            break;
        case COLLECTOR_REG:
            break;
        case ASSIGN_COLLECTOR:
            msg.setAssignCollector((AssignCollector) message);
            break;
        case READY_COLLECTOR:
            msg.setReadyCollector((ReadyCollector) message);
            break;
        case APP_ROLL:
            msg.setAppRoll((AppRoll) message);
            break;
        case RECOVERY_ROLL:
            msg.setRecoveryRoll((RecoveryRoll) message);
            break;
        case FAILURE:
            msg.setFailure((Failure) message);
            break;
        case UPLOAD_ROLL:
        case UPLOAD_SUCCESS:
        case UPLOAD_FAIL:
        case RECOVERY_SUCCESS:
        case RECOVERY_FAIL:
        case UNRECOVERABLE:
        case MANUAL_RECOVERY_ROLL:
            msg.setRollID((RollID) message);
            break;
        case RETIRESTREAM:
            msg.setStreamId((StreamID) message);
            break;
        case DUMPSTAT:
            break;
        case CONSUMER_REG:
            msg.setConsumerReg((ConsumerReg) message);
            break;
        case ASSIGN_CONSUMER:
            msg.setAssignConsumer((AssignConsumer) message);
            break;
        case OFFSET_COMMIT:
            msg.setOffsetCommit((OffsetCommit) message);
        default:
        }
        return msg.build();
    }
    
    public static Message wrapHeartBeat() {
        return wrapMessage(MessageType.HEARTBEART, null);
    }
    
    public static Message wrapNoAvailableNode(String app) {
        NoAvailableNode.Builder builder = NoAvailableNode.newBuilder();
        builder.setAppName(app);
        return wrapMessage(MessageType.NOAVAILABLENODE, builder.build());
    }
    
    public static Message wrapAppReg(String appName, String appServer, long regTs) {
        AppReg.Builder builder = AppReg.newBuilder();
        builder.setAppName(appName);
        builder.setAppServer(appServer);
        builder.setRegTs(regTs);
        return wrapMessage(MessageType.APP_REG, builder.build());
    }
    
    public static Message wrapCollectorReg() {
        return wrapMessage(MessageType.COLLECTOR_REG, null);
    }
    
    public static Message wrapAssignCollector(String appName, String collectorServer) {
        AssignCollector.Builder builder = AssignCollector.newBuilder();
        builder.setAppName(appName);
        builder.setCollectorServer(collectorServer);
        return wrapMessage(MessageType.ASSIGN_COLLECTOR, builder.build());
    }
    
    public static Message wrapReadyCollector(String app_name, String app_server, long peroid, String collector_server, long connectedTs) {
        ReadyCollector.Builder builder = ReadyCollector.newBuilder();
        builder.setAppName(app_name);
        builder.setAppServer(app_server);
        builder.setPeriod(peroid);
        builder.setCollectorServer(collector_server);
        builder.setConnectedTs(connectedTs);
        return wrapMessage(MessageType.READY_COLLECTOR, builder.build());
    }
    
    public static Message wrapAppRoll(String appName, String appServer,long period, long rollTs) {
        AppRoll.Builder builder = AppRoll.newBuilder();
        builder.setAppName(appName);
        builder.setAppServer(appServer);
        builder.setPeriod(period);
        builder.setRollTs(rollTs);
        return wrapMessage(MessageType.APP_ROLL, builder.build());
    }
    
    public static RollID wrapRollID(String appName, String appServer, long period, long rollTs) {
        RollID.Builder builder = RollID.newBuilder();
        builder.setAppName(appName);
        builder.setAppServer(appServer);
        if (period != 0) {
            builder.setPeriod(period);
        }
        builder.setRollTs(rollTs);
        return builder.build();
    }
    
    public static Message wrapUploadRoll(String appName, String appServer, long period, long rollTs) {
        return wrapMessage(MessageType.UPLOAD_ROLL, wrapRollID(appName, appServer, period, rollTs));
    }
    
    public static Message wrapUploadSuccess(String appName, String appServer, long rollTs) {
        return wrapMessage(MessageType.UPLOAD_SUCCESS, wrapRollID(appName, appServer, 0, rollTs));
    }
    
    public static Message wrapUploadFail(String appName, String appServer, long rollTs) {
        return wrapMessage(MessageType.UPLOAD_FAIL, wrapRollID(appName, appServer, 0, rollTs));
    }
    
    public static Message wrapRecoveryRoll(String appName, String collectorServer, long rollTs) {
        RecoveryRoll.Builder builder = RecoveryRoll.newBuilder();
        builder.setAppName(appName);
        builder.setCollectorServer(collectorServer);
        builder.setRollTs(rollTs);
        return wrapMessage(MessageType.RECOVERY_ROLL, builder.build());
    }
    
    public static Message wrapRecoverySuccess(String appName, String appServer, long rollTs) {
        return wrapMessage(MessageType.RECOVERY_SUCCESS, wrapRollID(appName, appServer, 0, rollTs));
    }
    
    public static Message wrapRecoveryFail(String appName, String appServer, long rollTs) {
        return wrapMessage(MessageType.RECOVERY_FAIL, wrapRollID(appName, appServer, 0, rollTs));
    }
    
    public static Message wrapFailure (String app, String appHost, NodeType type, long failTs) {
        Failure.Builder builder = Failure.newBuilder();
        builder.setApp(app);
        builder.setType(type);
        builder.setAppServer(appHost);
        builder.setFailTs(failTs);
        return wrapMessage(MessageType.FAILURE, builder.build());
    }
    
    public static Message wrapAppFailure (String app, String appHost, long failTs) {
       return wrapFailure(app, appHost, NodeType.APP_NODE, failTs);
    }
    
    public static Message wrapcollectorFailure (String app, String appHost, long failTs) {
        return wrapFailure(app, appHost, NodeType.COLLECTOR_NODE, failTs);
    }
    
    public static Message wrapUnrecoverable(String appName, String appServer, long rollTs) {
        return wrapMessage(MessageType.UNRECOVERABLE, wrapRollID(appName, appServer, 0, rollTs));
    }
    
    public static Message wrapManualRecoveryRoll(String appName, String appServer, long rollTs) {
        return wrapMessage(MessageType.MANUAL_RECOVERY_ROLL, wrapRollID(appName, appServer, 0, rollTs));
    }
    
    public static Message wrapRetireStream(String appName, String appServer) {
        StreamID.Builder builder = StreamID.newBuilder();
        builder.setAppName(appName);
        builder.setAppServer(appServer);
        return wrapMessage(MessageType.RETIRESTREAM, builder.build());
    }
    
    public static Message wrapDumpStat() {
        return wrapMessage(MessageType.DUMPSTAT, null);
    }

    /**
     * register consumer data to supervisor
     */
    public static Message wrapConsumerReg(final String consumerIdString, final String topic, 
            final int minConsumersInGroup) {
        ConsumerReg.Builder builder = ConsumerReg.newBuilder();
        builder.setConsumerIdString(consumerIdString);
        builder.setTopic(topic);
        builder.setMinConsumersInGroup(minConsumersInGroup);
        return wrapMessage(MessageType.CONSUMER_REG, builder.build());
    }

    /**
     * report committed offset of a partition
     */
    public static Message wrapOffsetCommit(String topic,
            String partitionName, long offset) {
        OffsetCommit.Builder builder = OffsetCommit.newBuilder();
        builder.setTopic(topic);
        builder.setPartition(partitionName);
        builder.setOffset(offset);
        return wrapMessage(MessageType.OFFSET_COMMIT, builder.build());
    }
    
    public static Message wrapAssignConsumer(String consumerId, String topic, ArrayList<PartitionInfo> partitionInfos) {
        AssignConsumer.Builder builder = AssignConsumer.newBuilder();
        builder.setConsumerIdString(consumerId);
        builder.setTopic(topic);
        
        for (PartitionInfo info : partitionInfos) {
            AssignConsumer.PartitionOffset.Builder infoBuilder = AssignConsumer.PartitionOffset.newBuilder();
            infoBuilder.setBrokerString(info.getConnection().getHost());
            infoBuilder.setPartitionName(info.getId());
            infoBuilder.setOffset(info.getEndOffset());
            builder.addPartitionOffsets(infoBuilder.build());
        }
        return wrapMessage(MessageType.ASSIGN_CONSUMER, builder.build());
    }
    
    public static Message wrapTopicReport(List<reportEntry> entryList) {
        TopicReport.Builder builder = TopicReport.newBuilder();
        for (reportEntry entry : entryList) {
            TopicReport.TopicEntry.Builder entryBuilder = TopicReport.TopicEntry.newBuilder();
            entryBuilder.setTopic(entry.topic);
            entryBuilder.setPartitionId(entry.partition);
            entryBuilder.setOffset(entry.offset);
            builder.addEntries(entryBuilder.build());
        }
        return wrapMessage(MessageType.TOPICREPORT, builder.build());
    }
}

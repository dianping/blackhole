package com.dp.blackhole.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.dp.blackhole.protocol.control.AppRegPB.AppReg;
import com.dp.blackhole.protocol.control.AppRollPB.AppRoll;
import com.dp.blackhole.protocol.control.AssignBrokerPB.AssignBroker;
import com.dp.blackhole.protocol.control.AssignConsumerPB.AssignConsumer;
import com.dp.blackhole.protocol.control.BrokerRegPB.BrokerReg;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes.AppConfRes;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes.LxcConfRes;
import com.dp.blackhole.protocol.control.ConsumerRegPB.ConsumerReg;
import com.dp.blackhole.protocol.control.DumpAppPB.DumpApp;
import com.dp.blackhole.protocol.control.DumpConsumerGroupPB.DumpConsumerGroup;
import com.dp.blackhole.protocol.control.DumpReplyPB.DumpReply;
import com.dp.blackhole.protocol.control.FailurePB.Failure;
import com.dp.blackhole.protocol.control.FailurePB.Failure.NodeType;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.MessagePB.Message.MessageType;
import com.dp.blackhole.protocol.control.NoAvailableNodePB.NoAvailableNode;
import com.dp.blackhole.protocol.control.OffsetCommitPB.OffsetCommit;
import com.dp.blackhole.protocol.control.QuitAndCleanPB.Clean;
import com.dp.blackhole.protocol.control.QuitAndCleanPB.InstanceGroup;
import com.dp.blackhole.protocol.control.QuitAndCleanPB.Quit;
import com.dp.blackhole.protocol.control.ReadyBrokerPB.ReadyBroker;
import com.dp.blackhole.protocol.control.RecoveryRollPB.RecoveryRoll;
import com.dp.blackhole.protocol.control.RemoveConfPB.RemoveConf;
import com.dp.blackhole.protocol.control.RestartPB.Restart;
import com.dp.blackhole.protocol.control.RollCleanPB.RollClean;
import com.dp.blackhole.protocol.control.RollIDPB.RollID;
import com.dp.blackhole.protocol.control.StreamIDPB.StreamID;
import com.dp.blackhole.protocol.control.TopicReportPB.TopicReport;
import com.google.protobuf.InvalidProtocolBufferException;

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
        case BROKER_REG:
            msg.setBrokerReg((BrokerReg) message);
            break;
        case ASSIGN_BROKER:
            msg.setAssignBroker((AssignBroker) message);
            break;
        case READY_BROKER:
            msg.setReadyBroker((ReadyBroker) message);
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
        case MAKR_UNRECOVERABLE:
            msg.setRollID((RollID) message);
            break;
        case RETIRESTREAM:
            msg.setStreamId((StreamID) message);
            break;
        case DUMPSTAT:
            break;
        case NOAVAILABLECONF:
            break;
        case CONF_REQ:
            break;
        case CONF_RES:
            msg.setConfRes((ConfRes) message);
            break;
        case DUMPCONF:
            break;
        case DUMPREPLY:
            msg.setDumpReply((DumpReply) message);
            break;
        case LISTAPPS:
            break;
        case LISTIDLE:
            break;
        case REMOVE_CONF:
            msg.setRemoveConf((RemoveConf) message);
            break;
        case DUMP_APP:
            msg.setDumpApp((DumpApp) message);
            break;
        case CONSUMER_REG:
        case CONSUMERREGFAIL:
            msg.setConsumerReg((ConsumerReg) message);
            break;
        case ASSIGN_CONSUMER:
            msg.setAssignConsumer((AssignConsumer) message);
            break;
        case OFFSET_COMMIT:
            msg.setOffsetCommit((OffsetCommit) message);
            break;
        case RESTART:
            msg.setRestart((Restart) message);
            break;
        case QUIT:
            msg.setQuit((Quit) message);
            break;
        case CLEAN:
            msg.setClean((Clean) message);
            break;
        case ROLL_CLEAN:
            msg.setRollClean((RollClean) message);
            break;
        case DUMP_CONSUMER_GROUP:
            msg.setDumpConsumerGroup((DumpConsumerGroup) message);
            break;
        case LIST_CONSUMER_GROUP:
            break;
        default:
        }
        return msg.build();
    }
    
    public static Message wrapHeartBeat() {
        return wrapMessage(MessageType.HEARTBEART, null);
    }
    
    public static Message wrapNoAvailableNode(String topic, String instanceId) {
        NoAvailableNode.Builder builder = NoAvailableNode.newBuilder();
        builder.setTopic(topic);
        if (instanceId != null) {
            builder.setInstanceId(instanceId);
        }
        return wrapMessage(MessageType.NOAVAILABLENODE, builder.build());
    }
    
    public static Message wrapTopicReg(String topic, String source, long regTs) {
        AppReg.Builder builder = AppReg.newBuilder();
        builder.setTopic(topic);
        builder.setSource(source);
        builder.setRegTs(regTs);
        return wrapMessage(MessageType.APP_REG, builder.build());
    }
    
    public static Message wrapBrokerReg(int brokerPort, int recoveryPort) {
        BrokerReg.Builder builder = BrokerReg.newBuilder();
        builder.setBrokerPort(brokerPort);
        builder.setRecoveryPort(recoveryPort);
        return wrapMessage(MessageType.BROKER_REG, builder.build());
    }
    
    public static Message wrapAssignBroker(String topic, String brokerServer, int port, String instanceId) {
        AssignBroker.Builder builder = AssignBroker.newBuilder();
        builder.setTopic(topic);
        builder.setBrokerServer(brokerServer);
        builder.setBrokerPort(port);
        if (instanceId != null) {
            builder.setInstanceId(instanceId);
        }
        return wrapMessage(MessageType.ASSIGN_BROKER, builder.build());
    }
    
    public static Message wrapReadyBroker(String topic, String source, long peroid, String broker_server, long connectedTs) {
        ReadyBroker.Builder builder = ReadyBroker.newBuilder();
        builder.setTopic(topic);
        builder.setSource(source);
        builder.setPeriod(peroid);
        builder.setBrokerServer(broker_server);
        builder.setConnectedTs(connectedTs);
        return wrapMessage(MessageType.READY_BROKER, builder.build());
    }
    
    public static Message wrapAppRoll(String topic, String source, long period, long rollTs) {
        AppRoll.Builder builder = AppRoll.newBuilder();
        builder.setTopic(topic);
        builder.setSource(source);
        builder.setPeriod(period);
        builder.setRollTs(rollTs);
        return wrapMessage(MessageType.APP_ROLL, builder.build());
    }
    
    public static RollID wrapRollID(String appName, String appServer, long period, long rollTs, boolean isFinal) {
        return wrapRollID(appName, appServer, period, rollTs, isFinal, "");
    }
    
    public static RollID wrapRollID(String topic, String source, long period, long rollTs, boolean isFinal, String compression) {
        RollID.Builder builder = RollID.newBuilder();
        builder.setTopic(topic);
        builder.setSource(source);
        builder.setPeriod(period);
        builder.setRollTs(rollTs);
        builder.setIsFinal(isFinal);
        builder.setCompression(compression);
        return builder.build();
    }
    
    public static Message wrapUploadRoll(String appName, String source, long period, long rollTs, boolean isFinal, String compression) {
        return wrapMessage(MessageType.UPLOAD_ROLL, wrapRollID(appName, source, period, rollTs, isFinal, compression));
    }
    
    public static Message wrapUploadSuccess(String appName, String appServer, long period, long rollTs, boolean isFinal, String compression) {
        return wrapMessage(MessageType.UPLOAD_SUCCESS, wrapRollID(appName, appServer, period, rollTs, isFinal, compression));
    }
    
    public static Message wrapUploadFail(String appName, String source, long period, long rollTs, boolean isFinal, String compression) {
        return wrapMessage(MessageType.UPLOAD_FAIL, wrapRollID(appName, source, period, rollTs, isFinal, compression));
    }
    
    public static Message wrapRecoveryRoll(String topic, String brokerServer, int port, long rollTs, String instanceId, boolean isFinal) {
        RecoveryRoll.Builder builder = RecoveryRoll.newBuilder();
        builder.setTopic(topic);
        builder.setBrokerServer(brokerServer);
        builder.setRecoveryPort(port);
        builder.setRollTs(rollTs);
        if (instanceId != null) {
            builder.setInstanceId(instanceId);
        }
        builder.setIsFinal(isFinal);
        return wrapMessage(MessageType.RECOVERY_ROLL, builder.build());
    }
    
    public static Message wrapRecoverySuccess(String appName, String source, long period, long rollTs, boolean isFinal) {
        return wrapMessage(MessageType.RECOVERY_SUCCESS, wrapRollID(appName, source, period, rollTs, isFinal));
    }
    
    public static Message wrapRecoveryFail(String appName, String source, long period, long rollTs, boolean isFinal) {
        return wrapMessage(MessageType.RECOVERY_FAIL, wrapRollID(appName, source, period, rollTs, isFinal));
    }
    
    public static Message wrapFailure (String topic, String source, NodeType type, long failTs) {
        Failure.Builder builder = Failure.newBuilder();
        builder.setTopic(topic);
        builder.setType(type);
        builder.setSource(source);
        builder.setFailTs(failTs);
        return wrapMessage(MessageType.FAILURE, builder.build());
    }
    
    public static Message wrapAppFailure (String app, String source, long failTs) {
       return wrapFailure(app, source, NodeType.APP_NODE, failTs);
    }
    
    public static Message wrapBrokerFailure (String app, String source, long failTs) {
        return wrapFailure(app, source, NodeType.BROKER_NODE, failTs);
    }
    
    public static Message wrapUnrecoverable(String appName, String source, long period, long rollTs, boolean isFinal) {
        return wrapMessage(MessageType.UNRECOVERABLE, wrapRollID(appName, source, period, rollTs, isFinal));
    }
    
    public static Message wrapManualRecoveryRoll(String appName, String source, long period, long rollTs) {
        return wrapMessage(MessageType.MANUAL_RECOVERY_ROLL, wrapRollID(appName, source, period, rollTs, false));
    }
    
    public static Message wrapRetireStream(String topic, String agentServer, String instanceId) {
        StreamID.Builder builder = StreamID.newBuilder();
        builder.setTopic(topic);
        builder.setAgentServer(agentServer);
        builder.setInstanceId(instanceId);
        return wrapMessage(MessageType.RETIRESTREAM, builder.build());
    }
    
    public static Message wrapDumpStat() {
        return wrapMessage(MessageType.DUMPSTAT, null);
    }

    public static Message wrapConfReq () {
        return wrapMessage(MessageType.CONF_REQ, null);
    }
    
    public static AppConfRes wrapAppConfRes(String topic, String watchFile,
            String period, String maxLineSize, String readInterval) {
        AppConfRes.Builder builder = AppConfRes.newBuilder();
        builder.setTopic(topic);
        builder.setPeriod(period);
        builder.setWatchFile(watchFile);
        if (maxLineSize != null) {
            builder.setMaxLineSize(maxLineSize);
        }
        builder.setReadInterval(readInterval);
        return builder.build();
    }
    
    public static LxcConfRes wrapLxcConfRes(String topic, String watchFile,
            String period, String maxLineSize, String readInterval, Set<String> ids) {
        LxcConfRes.Builder builder = LxcConfRes.newBuilder();
        builder.setTopic(topic);
        builder.setPeriod(period);
        builder.setWatchFile(watchFile);
        if (maxLineSize != null) {
            builder.setMaxLineSize(maxLineSize);
        }
        builder.setReadInterval(readInterval);
        builder.addAllInstanceIds(ids);
        return builder.build();
    }
    
    public static Message wrapConfRes (List<AppConfRes> appConfResList, List<LxcConfRes> lxcConfResList) {
        ConfRes.Builder builder = ConfRes.newBuilder();
        if (appConfResList != null) {
            builder.addAllAppConfRes(appConfResList);
        }
        if (lxcConfResList != null) {
            builder.addAllLxcConfRes(lxcConfResList);
        }
        return wrapMessage(MessageType.CONF_RES, builder.build());
    }
    
    public static Message wrapNoAvailableConf() {
        return wrapMessage(MessageType.NOAVAILABLECONF, null);
    }

    public static Message wrapDumpConf() {
        return wrapMessage(MessageType.DUMPCONF, null);
    }

    public static Message wrapDumpReply(String dumpReply) {
        DumpReply.Builder builder = DumpReply.newBuilder();
        builder.setReply(dumpReply);
        return wrapMessage(MessageType.DUMPREPLY, builder.build());
    }

    public static Message wrapListApps() {
        return wrapMessage(MessageType.LISTAPPS, null);
    }
    
    public static Message wrapListIdle() {
        return wrapMessage(MessageType.LISTIDLE, null);
    }

    public static Message wrapRemoveConf(String topic, ArrayList<String> agentServers) {
        RemoveConf.Builder builder = RemoveConf.newBuilder();
        builder.setTopic(topic);
        builder.addAllAgentServers(agentServers);
        return wrapMessage(MessageType.REMOVE_CONF, builder.build());
    }
    
    public static Message wrapDumpApp(String topic) {
        DumpApp.Builder builder = DumpApp.newBuilder();
        builder.setTopic(topic);
        return wrapMessage(MessageType.DUMP_APP, builder.build());
    }

    public static Message wrapMarkUnrecoverable(String appName, String source, long period, long rollTs) {
        return wrapMessage(MessageType.MAKR_UNRECOVERABLE, wrapRollID(appName, source, period, rollTs, false));
    }

    /**
     * register consumer data to supervisor
     */
    public static Message wrapConsumerReg(String group, String consumerId, String topic) {
        ConsumerReg.Builder builder = ConsumerReg.newBuilder();
        builder.setGroupId(group);
        builder.setConsumerId(consumerId);
        builder.setTopic(topic);
        return wrapMessage(MessageType.CONSUMER_REG, builder.build());
    }

    public static Message wrapConsumerRegFail(String group, String consumerId, String topic) {
        ConsumerReg.Builder builder = ConsumerReg.newBuilder();
        builder.setGroupId(group);
        builder.setConsumerId(consumerId);
        builder.setTopic(topic);
        return wrapMessage(MessageType.CONSUMERREGFAIL, builder.build());
    }
    
    /**
     * report committed offset of a partition
     */
    public static Message wrapOffsetCommit(String groupId, String consumerId, String topic, String partitionName, long offset) {
        OffsetCommit.Builder builder = OffsetCommit.newBuilder();
        builder.setGroupId(groupId);
        builder.setConsumerIdString(consumerId);
        builder.setTopic(topic);
        builder.setPartition(partitionName);
        builder.setOffset(offset);
        return wrapMessage(MessageType.OFFSET_COMMIT, builder.build());
    }
    
    public static AssignConsumer.PartitionOffset getPartitionOffset(String broker, String partition, long offset) {
        AssignConsumer.PartitionOffset.Builder infoBuilder = AssignConsumer.PartitionOffset.newBuilder();
        infoBuilder.setBrokerString(broker);
        infoBuilder.setPartitionName(partition);
        infoBuilder.setOffset(offset);
        return infoBuilder.build();
    }
    
    public static Message wrapAssignConsumer(String groupId, String consumerId, String topic, List<AssignConsumer.PartitionOffset> partitionOffsets) {
        AssignConsumer.Builder builder = AssignConsumer.newBuilder();
        builder.setGroup(groupId);
        builder.setConsumerIdString(consumerId);
        builder.setTopic(topic);
        
        for (AssignConsumer.PartitionOffset offset : partitionOffsets) {
            builder.addPartitionOffsets(offset);
        }
        return wrapMessage(MessageType.ASSIGN_CONSUMER, builder.build());
    }
    
    public static TopicReport.TopicEntry getTopicEntry(String topic, String partition, long offset) {
        TopicReport.TopicEntry.Builder entryBuilder = TopicReport.TopicEntry.newBuilder();
        entryBuilder.setTopic(topic);
        entryBuilder.setPartitionId(partition);
        entryBuilder.setOffset(offset);
        return entryBuilder.build();
    }
    
    public static Message wrapTopicReport(List<TopicReport.TopicEntry> entryList) {
        TopicReport.Builder builder = TopicReport.newBuilder();
        for (TopicReport.TopicEntry entry : entryList) {
            builder.addEntries(entry);
        }
        return wrapMessage(MessageType.TOPICREPORT, builder.build());
    }
    
    public static Message Buf2PB(ByteBuffer buf) throws InvalidProtocolBufferException {
        return Message.parseFrom(buf.array());
    }
    
    public static ByteBuffer PB2Buf(Message msg) {
        ByteBuffer buf = ByteBuffer.wrap(msg.toByteArray());
        return buf;
    }
    
    public static Message wrapRestart(ArrayList<String> agentServers) {
        Restart.Builder builder = Restart.newBuilder();
        builder.addAllAgentServers(agentServers);
        return wrapMessage(MessageType.RESTART, builder.build());
    }
    
    public static InstanceGroup wrapInstanceGroup(String topic, Set<String> ids) {
        InstanceGroup.Builder builder = InstanceGroup.newBuilder();
        builder.setTopic(topic);
        builder.addAllInstanceIds(ids);
        return builder.build();
    }
    
    public static Message wrapQuit(List<InstanceGroup> instanceGroup) {
        Quit.Builder builder = Quit.newBuilder();
        builder.addAllInstanceGroup(instanceGroup);
        return wrapMessage(MessageType.QUIT, builder.build());
    }
    
    public static Message wrapClean(List<InstanceGroup> instanceGroup) {
        Clean.Builder builder = Clean.newBuilder();
        builder.addAllInstanceGroup(instanceGroup);
        return wrapMessage(MessageType.CLEAN, builder.build());
    }
    
    public static Message wrapRollClean(String topic, String source, long period) {
        RollClean.Builder builder = RollClean.newBuilder();
        builder.setTopic(topic);
        builder.setSource(source);
        builder.setPeriod(period);
        return wrapMessage(MessageType.ROLL_CLEAN, builder.build());
    }
    
    public static Message wrapDumpConsumeGroup(String topic, String groupId) {
        DumpConsumerGroup.Builder builder = DumpConsumerGroup.newBuilder();
        builder.setTopic(topic);
        builder.setGroupId(groupId);
        return wrapMessage(MessageType.DUMP_CONSUMER_GROUP, builder.build());
    }
    
    public static Message wrapListConsumerGroups() {
        return wrapMessage(MessageType.LIST_CONSUMER_GROUP, null);
    }
}

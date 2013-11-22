package com.dp.blackhole.common;

import java.util.List;

import com.dp.blackhole.common.gen.AppRegPB.AppReg;
import com.dp.blackhole.common.gen.AppRollPB.AppRoll;
import com.dp.blackhole.common.gen.AssignCollectorPB.AssignCollector;
import com.dp.blackhole.common.gen.ConfResPB.ConfRes;
import com.dp.blackhole.common.gen.ConfResPB.ConfRes.AppConfRes;
import com.dp.blackhole.common.gen.DumpReplyPB.DumpReply;
import com.dp.blackhole.common.gen.FailurePB.Failure;
import com.dp.blackhole.common.gen.FailurePB.Failure.NodeType;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.MessagePB.Message.MessageType;
import com.dp.blackhole.common.gen.NoAvailableNodePB.NoAvailableNode;
import com.dp.blackhole.common.gen.ReadyCollectorPB.ReadyCollector;
import com.dp.blackhole.common.gen.RecoveryRollPB.RecoveryRoll;
import com.dp.blackhole.common.gen.RollIDPB.RollID;
import com.dp.blackhole.common.gen.StreamIDPB.StreamID;

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
    
    public static Message wrapAssignCollector(String appName, String collectorServer, int port) {
        AssignCollector.Builder builder = AssignCollector.newBuilder();
        builder.setAppName(appName);
        builder.setCollectorServer(collectorServer);
        builder.setCollectorPort(port);
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
    
    public static Message wrapRecoveryRoll(String appName, String collectorServer, int port, long rollTs) {
        RecoveryRoll.Builder builder = RecoveryRoll.newBuilder();
        builder.setAppName(appName);
        builder.setCollectorServer(collectorServer);
        builder.setCollectorPort(port);
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

    public static Message wrapConfReq () {
        return wrapMessage(MessageType.CONF_REQ, null);
    }
    
    public static AppConfRes wrapAppConfRes (String appName, String watchFile, String period, String bufSize) {
        AppConfRes.Builder builder = AppConfRes.newBuilder();
        builder.setAppName(appName);
        builder.setWatchFile(watchFile);
        if (period != null) {
            builder.setPeriod(period);
        }
        if (bufSize != null) {
            builder.setBufferSize(bufSize);
        }
        return builder.build();
    }
    
    public static Message wrapConfRes (List<AppConfRes> appConfResList) {
        ConfRes.Builder builder = ConfRes.newBuilder();
        builder.addAllAppConfRes(appConfResList);
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
}

package com.dp.blackhole.supervisor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;


import com.dp.blackhole.common.gen.AppRegPB.AppReg;
import com.dp.blackhole.common.gen.AppRollPB.AppRoll;
import com.dp.blackhole.common.Connection;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.gen.ConfResPB.ConfRes.AppConfRes;
import com.dp.blackhole.common.gen.FailurePB.Failure;
import com.dp.blackhole.common.gen.FailurePB.Failure.NodeType;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.MessagePB.Message.MessageType;
import com.dp.blackhole.common.gen.ReadyCollectorPB.ReadyCollector;
import com.dp.blackhole.common.gen.RollIDPB.RollID;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.conf.Context;


public class Supervisor {

    public static final Log LOG = LogFactory.getLog(Supervisor.class);
    
    Selector selector;
    volatile private boolean running = true;
    
    private BlockingQueue<Msg> messageQueue;
    private ConcurrentHashMap<Connection, ArrayList<Stream>> connectionMap;
    private ConcurrentHashMap<String, Connection> collectorNodes;
    private ConcurrentHashMap<String, Connection> appNodes;
    private ConcurrentHashMap<Stream, ArrayList<Stage>> Streams;
    private ConcurrentHashMap<StreamId, Stream> streamIdMap;
    private ZKHelper zkHelper;
    
    private class Msg {
        Message msg;
        Connection c;
    }
    
    private void send(Connection connection, Message msg) {
        SocketChannel channel = connection.getChannel();
        SelectionKey key = channel.keyFor(selector);
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        connection.offer(msg);
        LOG.debug("send message to " + connection.getHost() + " :" +msg);
        selector.wakeup();
    }
    
    protected void loop() {

        while (running) {
            SelectionKey key = null;
            try {
                selector.select();
                Iterator<SelectionKey> iter = selector.selectedKeys()
                        .iterator();
                while (iter.hasNext()) {
                    key = iter.next();
                    iter.remove();
                    if (key.isAcceptable()) {
                        ServerSocketChannel server = (ServerSocketChannel) key
                                .channel();
                        SocketChannel channel = server.accept();
                        if (channel == null) {
                            continue;
                        }
                        channel.configureBlocking(false);
                        Connection connection = new Connection(channel);
                        channel.register(selector, SelectionKey.OP_READ, connection);
                   
                        // TODO handle not null
                        if (connectionMap.get(connection) == null) {
                            ArrayList<Stream> streams = new ArrayList<Stream>();
                            connectionMap.put(connection, streams);
                            String remote = connection.getHost();
                            LOG.debug("client node "+remote+" connected");
                        } else {
                            LOG.error("connection already contained in connectionMap: " + connection.getHost());
                            connection.close();
                        }
                    } else if (key.isWritable()) {
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);

                        SocketChannel channel = (SocketChannel) key.channel();
                        Connection connection = (Connection) key.attachment();

                        while (true) {
                            ByteBuffer reply = connection.getWritebuffer();
                            if (reply == null) {
                                Message msg = connection.peek();
                                if (msg == null) {
                                    break;
                                }
                                byte[] data = msg.toByteArray();
                                reply = connection.createWritebuffer(4 + data.length);
                                reply.putInt(data.length);
                                reply.put(data);
                                reply.flip();
                            }
                            if (reply.remaining() == 0) {
                                connection.poll();
                                connection.resetWritebuffer();
                                continue;
                            }
                            int num = 0;
                            for (int i = 0; i < 16; i++) {
                                num = channel.write(reply);
                                if (num != 0) {
                                    break;
                                }
                            }
                            // socket buffer is full, register OP_WRITE, wait for next write
                            if (num == 0) {
                                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                            }
                        }
                    } else if (key.isReadable()) {
                        int count;
                        Connection connection = (Connection) key.attachment();
                        SocketChannel channel = (SocketChannel) key.channel();

                        ByteBuffer lengthBuf = connection.getLengthBuffer();
                        if (lengthBuf.hasRemaining()) {
                            count = channel.read(lengthBuf);
                            if (count < 0) {
                                closeConnection((Connection) key.attachment());
                                break;
                            } else if (lengthBuf.hasRemaining()) {
                                continue;
                            } else {
                                lengthBuf.flip();
                                int length = lengthBuf.getInt();
                                lengthBuf.clear();
                                connection.createDatabuffer(length);
                            }
                        }

                        ByteBuffer data = connection.getDataBuffer();
                        count = channel.read(data);
                        if (count < 0) {
                            closeConnection((Connection) key.attachment());
                            break;
                        }
                        if (data.remaining() == 0) {
                            data.flip();
                            Message msg = Message.parseFrom(data.array());
                            Msg event = new Msg();
                            event.c = connection;
                            event.msg = msg;
                            messageQueue.put(event);
                            if (msg.getType() != MessageType.HEARTBEART) {
                                LOG.debug("receive message from " + connection.getHost() +" :" + msg);
                            }
                            lengthBuf.clear();
                        }
                    }
                    key = null;
                }
            } catch (InterruptedException ie) {
                LOG.info("got InterruptedException", ie);
            } catch (IOException e) {
                closeConnection((Connection) key.attachment());
            }
        }
    }

    /*
     * cancel the key, remove it from appNodes or collectorNodes, then revisit streams
     * 1. appNode fail, mark the stream as inactive, mark all the stages as pending unless the uploading stage
     * 2. collectorNode fail, reassign collector if it is current stage, mark the stream as pending when no available collector;
     *  do recovery if the stage is not current stage, mark the stage as pending when no available collector
     */
    private void closeConnection(Connection connection) {
        SelectionKey key = connection.getChannel().keyFor(selector);
        LOG.info(key + "is closed, socketChannel is " + connection.getChannel());
        key.cancel();
        
        long now = Util.getTS();
        String host = connection.getHost();
        if (connection.getNodeType() == Connection.APPNODE) {
            appNodes.remove(host);
        } else if (connection.getNodeType() == Connection.COLLECTORNODE) {
            collectorNodes.remove(host);
        }
        ArrayList<Stream> streams = connectionMap.get(connection);
        synchronized (streams) {
            for (Stream stream : streams) {
                if (connection.getNodeType() == Connection.APPNODE) {
                    stream.updateActive(false);
                }
                ArrayList<Stage> stages = Streams.get(stream);
                synchronized (stages) {
                    for (Stage stage : stages) {
                        if (connection.getNodeType() == Connection.APPNODE && stage.status != Stage.UPLOADING) {
                            Issue e = new Issue();
                            e.desc = "logreader failed";
                            e.ts = now;
                            stage.issuelist.add(e);
                            stage.status = Stage.PENDING;
                        } else if (connection.getNodeType() == Connection.COLLECTORNODE) {
                            Issue e = new Issue();
                            e.desc = "collector failed";
                            e.ts = now;
                            stage.issuelist.add(e);
                            stage.status = Stage.COLLECTORFAIL;
                            if (stage.isCurrent()) {
                                // do not reassign collector here, since logreader will find collector fail,
                                // and do appReg again; otherwise two appReg for the same stream will send 
                                // String newCollector = assignCollector(stage.app, appNodes.get(stage.apphost));
                                // if (newCollector == null) {
                                //    stage.status = Stage.PENDING;
                                // }
                            } else {
                                String newCollector = doRecovery(stream, stage);
                                if (newCollector != null) {
                                    stage.status = Stage.RECOVERYING;
                                    stage.collectorhost = newCollector;
                                }
                            }
                        }
                    }
                }
            }
        }
        connectionMap.remove(connection);
        if (connection != null) {
            connection.close();
        }
    }

    /*
     * failure happened only on stream, that is, current stage
     * just log it in a issue, because:
     * if it is a collector fail, the corresponding log reader should find it 
     * and ask for a new collector; if it is a appnode fail, the appNode will
     * register the app again 
     */
    private void handlerFailure(Failure failure) {        
        StreamId id = new StreamId(failure.getApp(), failure.getAppServer());
        Stream stream = streamIdMap.get(id);
        
        ArrayList<Stage> stages = Streams.get(stream);
        synchronized (stages) {
            Stage current = null;
            if (stages.size() != 0) {
            current = stages.get(stages.size() - 1);
            } else {
                LOG.error("no stages found on stream: " + stream);
                return;
            }
            if (failure.getType() == NodeType.APP_NODE) {
                Issue i = new Issue();
                i.ts = failure.getFailTs();
                i.desc = "logreader failed";
                current.issuelist.add(i);
            } else {
                Issue i = new Issue();
                i.ts = failure.getFailTs();
                i.desc = "collector failed";
                current.issuelist.add(i);
            }
        }
    }
    
    /*
     * try to do recovery again when last recovery failed
     */
    private void handleRecoveryFail(RollID rollID) {
        StreamId id = new StreamId(rollID.getAppName(), rollID.getAppServer());
        Stream stream = streamIdMap.get(id);
        
        if (stream != null) {
        ArrayList<Stage> stages = Streams.get(stream);
        synchronized (stages) {
            for (Stage stage : stages) {
                if (stage.rollTs == rollID.getRollTs()) {
                    String newCollector = doRecovery(stream, stage);
                    stage.collectorhost = newCollector;
                    break;
                }
            }
        }
        } else {
            LOG.error("can't find stream by streamid: " + id);
            if (LOG.isDebugEnabled()) {
                for(Entry<StreamId, Stream> e : streamIdMap.entrySet()) {
                    LOG.debug("key: " + e.getKey());
                    LOG.debug("key: " + e.getValue());
                }
            }
        }
    }

    /*
     * mark the stage as uploaded , print summary and remove it from Streams
     */
    private void handleRecoverySuccess(RollID rollID) {
        StreamId id = new StreamId(rollID.getAppName(), rollID.getAppServer());
        Stream stream = streamIdMap.get(id);

        if (stream != null) {
            stream.setGreatlastSuccessTs(rollID.getRollTs());
            ArrayList<Stage> stages = Streams.get(stream);
            synchronized (stages) {
                for (Stage stage : stages) {
                    if (stage.rollTs == rollID.getRollTs()) {
                        stage.status = Stage.UPLOADED;
                        LOG.info(stage.toString());
                        stages.remove(stage);
                        break;
                    }
                }
            }
        } else {
            LOG.error("can't find stream by streamid: " + id);
            if (LOG.isDebugEnabled()) {
                for(Entry<StreamId, Stream> e : streamIdMap.entrySet()) {
                    LOG.debug("key: " + e.getKey());
                    LOG.debug("key: " + e.getValue());
                }
            }
        }
    }

    /*
     * mark the upload failed stage as recovery
     * add issue to the stage, and do recovery
     */
    private void handleUploadFail(RollID rollID) {
        StreamId id = new StreamId(rollID.getAppName(), rollID.getAppServer());
        Stream stream = streamIdMap.get(id);

        if (stream != null) {
            ArrayList<Stage> stages = Streams.get(stream);
            Issue e = new Issue();
            e.desc = "upload failed";
            e.ts = Util.getTS();
            synchronized (stages) {
                for (Stage stage : stages) {
                    if (stage.rollTs == rollID.getRollTs()) {
                        stage.status = Stage.RECOVERYING;
                        stage.issuelist.add(e);
                        String newCollector = doRecovery(stream, stage);
                        stage.collectorhost = newCollector;
                        break;
                    }
                }
            }
        } else {
            LOG.error("can't find stream by streamid: " + id);
            if (LOG.isDebugEnabled()) {
                for(Entry<StreamId, Stream> e : streamIdMap.entrySet()) {
                    LOG.debug("key: " + e.getKey());
                    LOG.debug("key: " + e.getValue());
                }
            }
        }
    }

    /*
     * update the stream's lastSuccessTs
     * make the uploaded stage as uploaded and remove it from Streams
     */
    private void handleUploadSuccess(RollID rollID, Connection from) {
        StreamId id = new StreamId(rollID.getAppName(), rollID.getAppServer());
        Stream stream = streamIdMap.get(id);
        
        if (stream != null) {
        stream.setGreatlastSuccessTs(rollID.getRollTs()); 
       
        ArrayList<Stage> stages = Streams.get(stream);
        if (stages != null) {
            synchronized (stages) {
                for (Stage stage : stages) {
                    if (stage.rollTs == rollID.getRollTs()) {
                        stage.status = Stage.UPLOADED;
                        LOG.info(stage.toString());
                        stages.remove(stage);
                        break;
                    }
                }
            }
        }
        } else {
            LOG.error("can't find stream by streamid: " + id);
            if (LOG.isDebugEnabled()) {
                for(Entry<StreamId, Stream> e : streamIdMap.entrySet()) {
                    LOG.debug("key: " + e.getKey());
                    LOG.debug("key: " + e.getValue());
                }
            }
        }
    }
    
    /*
     * current stage rolled
     * do recovery if current stage is not clean start or some issues happpened,
     * or upload the rolled stage;
     * create next stage as new current stage
     */
    private void handleRolling(AppRoll msg, Connection from) {
        StreamId id = new StreamId(msg.getAppName(), msg.getAppServer());
        Stream stream = streamIdMap.get(id);
        
        ArrayList<Stage> stages = Streams.get(stream);
        if (stages != null) {
            synchronized (stages) {
                Stage current = null;
                for (Stage stage : stages) {
                    if (stage.rollTs == msg.getRollTs()) {
                        current = stage;
                    }
                }
                if (current == null) {
                    LOG.error("Stage not found: " + stream.app + "@" + stream.appHost + " rollTs " + msg.getRollTs());
                    return;
                } else {
                    if (current.cleanstart == false || current.issuelist.size() != 0) {
                        current.status = Stage.RECOVERYING;
                        current.isCurrent = false;
                        String newCollector = doRecovery(stream, current);
                        current.collectorhost = newCollector;
                    } else {
                        current.status = Stage.UPLOADING;
                        current.isCurrent = false;
                        doUpload(stream, current, from);
                    }
                }
                Stage next = new Stage();
                next.app = stream.app;
                next.apphost = stream.appHost;
                next.collectorhost = current.collectorhost;
                next.cleanstart = true;
                next.rollTs = current.rollTs + stream.period * 1000;
                next.status = Stage.APPENDING;
                next.isCurrent = true;
                next.issuelist = new ArrayList<Issue>();
                stages.add(next);
            }
        } else {
            LOG.error("can't find stream by streamid: " + id);
            if (LOG.isDebugEnabled()) {
                for(Entry<StreamId, Stream> e : streamIdMap.entrySet()) {
                    LOG.debug("key: " + e.getKey());
                    LOG.debug("key: " + e.getValue());
                }
            }
        }
    }

    private String doUpload(Stream stream, Stage current, Connection from) {
        Message message = PBwrap.wrapUploadRoll(current.app, current.apphost, stream.period, current.rollTs);
        send(from, message);
        return from.getHost();
    }

    /*
     * do recovery of one stage of a stream
     * if the stream is not active or no collector now,
     * mark the stream as pending
     * else send recovery message
     */
    private String doRecovery(Stream stream, Stage stage) {
        String collector = null;
        collector = getCollector();   

        if (collector == null || !stream.isActive()) {
            stage.status = Stage.PENDING;
        } else {
            Connection c = appNodes.get(stream.appHost);
            Message message = PBwrap.wrapRecoveryRoll(stream.app, collector, stage.rollTs);
            send(c, message);
        }

        return collector;
    }
    
    /*
     * record the stream if it is a new stream;
     * do recovery if it is a old stream
     *  1. recovery appfail stages (include current stage, but do no recovery)
     *  2. recovery missed appfail stages (include current stage, but do no recovery)
     *  3. recovery current stage with collector fail
     */
    private void registerStream(ReadyCollector message, Connection from) {
        long connectedTs = message.getConnectedTs();
        long currentTs = Util.getRollTs(connectedTs, message.getPeriod());
        
        StreamId id = new StreamId(message.getAppName(), message.getAppServer());
        Stream stream = streamIdMap.get(id);
        
        if (stream == null) {
            // new stream
            stream = new Stream();
            stream.app = message.getAppName();
            stream.appHost = message.getAppServer();
            stream.startTs = connectedTs;
            stream.period = message.getPeriod();
            stream.setlastSuccessTs(currentTs - stream.period * 1000);
            
            ArrayList<Stage> stages = new ArrayList<Stage>();
            Stage current = new Stage();
            current.app = stream.app;
            current.apphost = stream.appHost;
            current.collectorhost = message.getCollectorServer();
            current.cleanstart = false;
            current.issuelist = new ArrayList<Issue>();
            current.status = Stage.APPENDING;
            current.rollTs = currentTs;
            current.isCurrent = true;
            stages.add(current);
            Streams.put(stream, stages);
            streamIdMap.put(id, stream);
            // record the stream with appHost connection
            Connection appc = appNodes.get(stream.appHost); 
            List<Stream> streamsWithinAppHost = connectionMap.get(appc);
            if (streamsWithinAppHost != null) {
                synchronized (streamsWithinAppHost) {
                    streamsWithinAppHost.add(stream);
                }
            } else {
                LOG.fatal("unregistered appHost connection: " + appc.getHost());
                return;
            }
            // record the stream with collectorHost connection
            List<Stream> streamsWithinCollectorHost = connectionMap.get(from);
            if (streamsWithinCollectorHost != null) {
                synchronized (streamsWithinCollectorHost) {
                    streamsWithinCollectorHost.add(stream);
                }
            } else {
                LOG.fatal("unregistered collectorHost connection: " + appc.getHost());
                return;
            }
        } else {
            if (!stream.isActive()) {
                stream.updateActive(true);
            }
            
            // old stream with app fail
            Issue issue = new Issue();
            issue.ts = connectedTs;
            issue.desc = "stream reconnected";

            
            ArrayList<Stage> stages = Streams.get(stream);
            synchronized (stages) {
             // recovery Pending stages
                for (int i=0 ; i < stages.size(); i++) {
                    Stage stage = stages.get(i);
                    LOG.debug(stage.toString());
                    if (stage.status == Stage.PENDING || stage.status == Stage.COLLECTORFAIL) {
                        stage.issuelist.add(issue);
                        // do not recovery current stage
                        if (stage.rollTs != currentTs) {
                            stage.status = Stage.RECOVERYING;
                            String newCollector = doRecovery(stream, stage);
                            stage.collectorhost = newCollector;
                        } else {
                            // fix current stage status
                            stage.status = Stage.APPENDING;
                            stage.collectorhost = message.getCollectorServer();
                        }
                    }
                }

                // recovery missed Pending stages
                int missedStageCount = getMissedStageCount(stream, connectedTs);
                if (missedStageCount != 0) {
                    if (stages.size() != 0) {
                        Stage current = stages.get(stages.size() - 1);
                        current.isCurrent = false;
                    } else {
                        LOG.error("no stages found on stream: " + stream);
                        return;
                    }
                    ArrayList<Stage> missedStages = getMissedStages(stream, missedStageCount, issue);
                    for (Stage stage : missedStages) {
                        LOG.debug("missed pending stages: " + stage.toString());
                        // check whether it is missed
                        if (!stages.contains(stage)) {
                            LOG.info("process missed stage: " + stage.rollTs +" ("+Util.ts2String(stage.rollTs)+")");
                            // do not recovery current stage
                            if (stage.rollTs != currentTs) {
                                stage.status = Stage.RECOVERYING;
                                String newCollector = doRecovery(stream, stage);
                                stage.collectorhost = newCollector;
                            } else {
                                stage.status = Stage.APPENDING;
                                stage.collectorhost = message.getCollectorServer();
                            }
                            stages.add(stage);
                        } else {
                            LOG.info("process not really missed stage: " + stage.rollTs +" ("+Util.ts2String(stage.rollTs)+")");
                            int index = stages.indexOf(stage);
                            Stage nmStage = stages.get(index);
//                            nmStage.issuelist.add(issue);
                            if (nmStage.rollTs != currentTs) {
                                nmStage.isCurrent = false;
                            }
                        }
                    }
                }
            }
        }
    }
    
    private int getMissedStageCount(Stream stream, long connectedTs) {
        long rollts = Util.getRollTs(connectedTs, stream.period);
        return (int) ((rollts - stream.getlastSuccessTs()) / stream.period / 1000);
    }
    
    /*
     * caller must hold the monitor of stages
     */
    private ArrayList<Stage> getMissedStages(Stream stream, int missedStageCount, Issue issue) {
        ArrayList<Stage> missedStages = new ArrayList<Stage>();
        for (int i = 0; i< missedStageCount; i++) {
            Stage stage = new Stage();
            stage.app = stream.app;
            stage.apphost = stream.appHost;
            if (i == missedStageCount-1) {
                stage.isCurrent = true;
            } else {
                stage.isCurrent = false;
            }
            stage.issuelist = new ArrayList<Issue>();
            stage.issuelist.add(issue);
            stage.status = Stage.RECOVERYING;
            stage.rollTs = stream.getlastSuccessTs() + stream.period * 1000 * (i+1);
            missedStages.add(stage);
        }
        return missedStages;
    }
    
    /*
     * 1. recored the connection in appNodes
     * 2. assign a collect to the app
     */
    private void registerApp(Message m, Connection from) throws InterruptedException {
        from.setNodeType(Connection.APPNODE);
        if (appNodes.get(from.getHost()) == null) {
            appNodes.put(from.getHost(), from);
            LOG.info("AppNode " + from.getHost() + " registered");
        }
        AppReg message = m.getAppReg();
        assignCollector(message.getAppName(), from);
    }

    private String assignCollector(String app, Connection from) {
        String collector = getCollector();
        Message message;
        if (collector != null) {
            message = PBwrap.wrapAssignCollector(app, collector);
        } else {
            message = PBwrap.wrapNoAvailableNode();
        }

        send(from, message);
        return collector;
    }
    
    /*
     * 1. record the connection in collectorNodes
     * 2. recovery Pending stages (include current stage, but do no recovery)
     * 3. recovery missed Pending stages (include current stage, but do no recovery)
     */
    private void registerCollectorNode(Connection from) {
        from.setNodeType(Connection.COLLECTORNODE);
        collectorNodes.put(from.getHost(), from);
        
        long now = Util.getTS();
        Issue issue = new Issue();
        issue.ts = now;
        issue.desc = "avialable collector restored";
        
        if (collectorNodes.size() == 1) {
            for (Entry<Stream, ArrayList<Stage>> e : Streams.entrySet()) {
                Stream stream = e.getKey();
                if (stream.isActive()) {
                    // recovery Pending stages
                    LOG.debug("process Pending stages");
                    long currentTs = Util.getRollTs(now, stream.period);
                    ArrayList<Stage> stages = e.getValue();
                    synchronized (stages) {
                        for (int i=0 ; i < stages.size(); i++) {
                            Stage stage = stages.get(i);
                            LOG.debug(stage.toString());
                            if (stage.status == Stage.PENDING) {
                                stage.status = Stage.RECOVERYING;
                                stage.issuelist.add(issue);
                                // do not recovery current stage
                                if (stage.rollTs != currentTs) {
                                    String newCollector = doRecovery(stream, stage);
                                    stage.collectorhost = newCollector;
                                }
                            }
                        }
                    }
                    // recovery missed Pending stages
                    int missedStageCount = getMissedStageCount(stream, now);
                    LOG.info("need recovery missed Pending stages: " + missedStageCount);
                    if (missedStageCount != 0) {
                        if (stages.size() != 0) {
                            Stage current = stages.get(stages.size() - 1);
                            current.isCurrent = false;
                        } else {
                            LOG.error("no stages found on stream: " + stream);
                            continue;
                        }
                        ArrayList<Stage> missedStages = getMissedStages(stream, missedStageCount, issue);
                        for (Stage stage : missedStages) {
                            LOG.debug("missed pending stages: " + stage.toString());
                            // check whether it is missed
                            if (!stages.contains(stage)) {
                                LOG.info("process missed stage: " + stage.rollTs + " ("+Util.ts2String(stage.rollTs)+")");
                                // do not recovery current stage
                                if (stage.rollTs != currentTs) {
                                    String newCollector = doRecovery(stream, stage);
                                    stage.collectorhost = newCollector;
                                }
                                stages.add(stage);
                            } else {
                                LOG.info("process not really missed stage: " + stage.rollTs + " ("+Util.ts2String(stage.rollTs)+")");
                                int index = stages.indexOf(stage);
                                Stage nmStage = stages.get(index);
//                                nmStage.issuelist.add(issue);
                                if (nmStage.rollTs != currentTs) {
                                    nmStage.isCurrent = false;
                                }
                            }
                        }
                    }
                }
            }
        }

    }

    /* 
     * random return a collector
     * if no collectorNodes now, return null
     */
    private String getCollector() {
        ArrayList<String> array = new ArrayList<String>(collectorNodes.keySet());
        if (array.size() == 0 ) {
            return null;
        }
        Random random = new Random();
        String collector = array.get(random.nextInt(array.size()));
        return collector;
    }
    
    private class Handler extends Thread {
        private boolean running= true;

        @Override
        public void run() {
            while (running) {
                Msg e;
                try {
                    e = messageQueue.take();
                    process(e.msg, e.c);
                } catch (InterruptedException ie) {
                    LOG.info("handler thread interrupted");
                    running = false;
                }
            }
        }
        
        private void process(Message msg, Connection from) throws InterruptedException {

            switch (msg.getType()) {
            case HEARTBEART:
                from.updateHeartBeat();
                break;
            case CONF_REQ:
                emitConf(from);
                break;
            case COLLECTOR_REG:
                registerCollectorNode(from);
                break;
            case APP_REG:
                registerApp(msg, from);
                break;
            case READY_COLLECTOR:
                registerStream(msg.getReadyCollector(), from);
                break;
            case APP_ROLL:
                handleRolling(msg.getAppRoll(), from);
                break;
            case UPLOAD_SUCCESS:
                handleUploadSuccess(msg.getRollID(), from);
                break;
            case UPLOAD_FAIL:
                handleUploadFail(msg.getRollID());
                break;
            case RECOVERY_SUCCESS:
                handleRecoverySuccess(msg.getRollID());
                break;
            case RECOVERY_FAIL:
                handleRecoveryFail(msg.getRollID());
                break;
            case FAILURE:
                handlerFailure(msg.getFailure());
                break;
            default:
                LOG.warn("unknown message: " + msg.toString());
            }
        }
    }
    
    private class LiveChecker extends Thread {
        boolean running = true;
        
        @Override
        public void run() {
            int THRESHOLD = 15 * 1000;
            while (running) {
                try {
                    Thread.sleep(5000);
                    long now = Util.getTS();
                    for (Connection c : connectionMap.keySet()) {
                        if (now - c.getLastHeartBeat() > THRESHOLD) {
                            closeConnection(c);
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.info("LiveChecker thread interrupted");
                    running =false;
                }
            }
        }
    }
    
    private void initializeZK() throws FileNotFoundException, IOException, 
            KeeperException, InterruptedException {
        Properties prop = new Properties();
        prop.load(new FileReader(new File("config.properties")));
        String connectString = prop.getProperty(ParamsKey.ZKServer.ZK_HOST_LIST);
        int sessionTimeout = Integer.parseInt(prop.getProperty(ParamsKey.ZKServer.ZK_CONNECT_TIMEOUT));
        zkHelper = ZKHelper.getInstance();
        zkHelper.createConnection(connectString, sessionTimeout);
        if (zkHelper.notExist(ParamsKey.ZNode.ROOT))
            throw new IOException("There is no root node \"/blackhole\" in zookeeper tree");
        if (zkHelper.notExist(ParamsKey.ZNode.STREAMS))
            throw new IOException("There is no stream node \"/blackhole/steams\" in zookeeper tree");
        zkHelper.loadAllConfs();
    }
    
    private void init() throws IOException, ClosedChannelException, 
            KeeperException, InterruptedException {
        initializeZK();
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ServerSocket ss = ssc.socket();
        ss.bind(new InetSocketAddress(8080));
        selector = Selector.open();
        ssc.register(selector, SelectionKey.OP_ACCEPT);
        connectionMap = new ConcurrentHashMap<Connection, ArrayList<Stream>>();
        collectorNodes = new ConcurrentHashMap<String, Connection>();
        appNodes =  new ConcurrentHashMap<String, Connection>();
        messageQueue = new LinkedBlockingQueue<Msg>();
        Streams = new ConcurrentHashMap<Stream, ArrayList<Stage>>();
        streamIdMap = new ConcurrentHashMap<StreamId, Stream>();
        Handler handler = new Handler();
        handler.start();
        LiveChecker checker = new LiveChecker();
        checker.start();
    }
    
    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws Exception {
        Supervisor supervisor = new Supervisor();
        supervisor.init();
        supervisor.loop();
    }

    private void emitConf(Connection from) {
        List<AppConfRes> appConfResList = new ArrayList<AppConfRes>();
        List<String> appNames = zkHelper.appHostToAppNames.get(from.getHost());
        for (String appName : appNames) {
            Context context = ConfigKeeper.configMap.get(appName);
            String watchFile = context.getString(ParamsKey.Appconf.WATCH_FILE);
            String period = context.getString(ParamsKey.Appconf.ROLL_PERIOD);
            AppConfRes appConfRes = PBwrap.wrapAppConfRes(appName, watchFile, period);
            appConfResList.add(appConfRes);
        }
        Message message = PBwrap.wrapConfRes(appConfResList);
        send(from, message);
    }
    
    public void emitConfToAll() {
        List<AppConfRes> appConfResList = new ArrayList<AppConfRes>();
        for (String host : zkHelper.appHostToAppNames.keySet()) {
            List<String> appNames = zkHelper.appHostToAppNames.get(host);
            for (String appName : appNames) {
                Context context = ConfigKeeper.configMap.get(appName);
                String watchFile = context.getString(ParamsKey.Appconf.WATCH_FILE);
                String period = context.getString(ParamsKey.Appconf.ROLL_PERIOD);
                AppConfRes appConfRes = PBwrap.wrapAppConfRes(appName, watchFile, period);
                appConfResList.add(appConfRes);
            }
            Message message = PBwrap.wrapConfRes(appConfResList);
            send(appNodes.get(host), message);
        }
    }
}

package com.dp.blackhole.supervisor;

import java.io.File;
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
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.gen.AppRegPB.AppReg;
import com.dp.blackhole.common.gen.AppRollPB.AppRoll;
import com.dp.blackhole.common.Connection;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.gen.FailurePB.Failure;
import com.dp.blackhole.common.gen.FailurePB.Failure.NodeType;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.MessagePB.Message.MessageType;
import com.dp.blackhole.common.gen.ReadyCollectorPB.ReadyCollector;
import com.dp.blackhole.common.gen.RollIDPB.RollID;
import com.dp.blackhole.common.gen.StreamIDPB.StreamID;
import com.dp.blackhole.common.Util;

public class Supervisor {

    public static final Log LOG = LogFactory.getLog(Supervisor.class);
    
    private Selector selector;
    volatile private boolean running = true;
    private Handler[] handlers = null;
    private int handlerCount = 1;
    
    private BlockingQueue<Msg> messageQueue;
    private ConcurrentHashMap<Connection, ArrayList<Stream>> connectionStreamMap;
    private ConcurrentHashMap<Stage, Connection> stageConnectionMap;
    private ConcurrentHashMap<String, Connection> collectorNodes;
    private ConcurrentHashMap<String, Connection> appNodes;
    private ConcurrentHashMap<Stream, ArrayList<Stage>> Streams;
    private ConcurrentHashMap<StreamId, Stream> streamIdMap;
    
    private class Msg {
        Message msg;
        Connection c;
    }
    
    private void send(Connection connection, Message msg) {
        if (connection == null || !connection.isActive()) {
            LOG.error("connection is null or closed, message sending abort: " + msg);
            return;
        }
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
                   
                        if (connectionStreamMap.get(connection) == null) {
                            ArrayList<Stream> streams = new ArrayList<Stream>();
                            connectionStreamMap.put(connection, streams);
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
                                break;
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
                                continue;
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
                            continue;
                        }
                        if (data.remaining() == 0) {
                            data.flip();
                            Message msg = Message.parseFrom(data.array());
                            if (msg.getType() != MessageType.HEARTBEART) {
                                LOG.debug("receive message from " + connection.getHost() +" :" + msg);
                            }
                            Msg event = new Msg();
                            event.c = connection;
                            event.msg = msg;
                            messageQueue.put(event);
                        }
                    }
                }
            } catch (InterruptedException ie) {
                LOG.warn("got InterruptedException", ie);
            } catch (IOException e) {
                closeConnection((Connection) key.attachment());
            }
        }
    }
    
    /*
     * mark the stream as inactive, mark all the stages as pending unless the uploading stage
     * remove the relationship of the corresponding collectorNode and streams
     */
    private void handleAppNodeFail(Connection connection, long now) {
        ArrayList<Stream> streams = connectionStreamMap.get(connection);
        if (streams != null) {
            for (Stream stream : streams) {
                LOG.info("mark stream as inactive: " + stream);
                stream.updateActive(false);
                ArrayList<Stage> stages = Streams.get(stream);
                if (stages != null) {
                    synchronized (stages) {
                        for (Stage stage : stages) {
                            LOG.info("checking stage: " + stage);
                            if (stage.status != Stage.UPLOADING) {
                                Issue e = new Issue();
                                e.desc = "logreader failed";
                                e.ts = now;
                                stage.issuelist.add(e);
                                stage.status = Stage.PENDING;
                            }
                        }
                    }
                }
                
                // remove corresponding collectorNodes's relationship with the stream
                String collectorHost = stream.getCollectorHost();
                Connection collectorConnection = collectorNodes.get(collectorHost);
                if (collectorConnection != null) {
                    ArrayList<Stream> associatedStreams = connectionStreamMap.get(collectorConnection);
                    if (associatedStreams != null) {
                        synchronized (associatedStreams) {
                            associatedStreams.remove(stream);
                            if (associatedStreams.size() == 0) {
                                connectionStreamMap.remove(collectorConnection);
                            }
                        }
                    }
                }
            }
        } else {
            LOG.warn("can not get associate streams from connectionStreamMap by connection: " + connection);
        }
    }
    
    /*
     * 1. process current stage on associated streams
     * 2. remove the relationship of the corresponding appNode and streams
     * 3. processing uploading and recovery stages
     */
    private void handleCollectorNodeFail(Connection connection, long now) {
        ArrayList<Stream> streams = connectionStreamMap.get(connection);
        // processing current stage on streams
        if (streams != null) {
            for (Stream stream : streams) {
                ArrayList<Stage> stages = Streams.get(stream);
                synchronized (stages) {
                    Stage current = stages.get(stages.size() -1);
                    if (!current.isCurrent()) {
                        LOG.error("stage " + current + "should be current stage");
                        continue;
                    }
                    LOG.info("checking current stage: " + current);
                    
                    Issue e = new Issue();
                    e.desc = "collector failed";
                    e.ts = now;
                    current.issuelist.add(e);

                    // do not reassign collector here, since logreader will find collector fail,
                    // and do appReg again; otherwise two appReg for the same stream will send 
                    if (collectorNodes.size() == 0) {
                        current.status = Stage.PENDING;
                    } else {
                        current.status = Stage.COLLECTORFAIL;
                    }
                    LOG.info("after checking current stage: " + current);
                }
                   
                // remove corresponding appNodes's relationship with the stream
                String appHost = stream.appHost;
                Connection appConnection = appNodes.get(appHost);
                if (appConnection != null) {
                    ArrayList<Stream> associatedStreams = connectionStreamMap.get(appConnection);
                    if (associatedStreams != null) {
                        synchronized (associatedStreams) {
                            associatedStreams.remove(stream);
                            if (associatedStreams.size() == 0) {
                                connectionStreamMap.remove(appConnection);
                            }
                        }
                    }
                }
            }
        } else {
            LOG.warn("can not get associate streams from connectionStreamMap by connection: " + connection);
        }
        
        // processing uploading and recovery stages
        for (Entry<Stage, Connection> entry : stageConnectionMap.entrySet()) {
            if (connection.equals(entry.getValue())) {
                LOG.info("processing entry: "+ entry);
                Stage stage = entry.getKey();
                if (stage.status == Stage.PENDING) {
                    continue;
                }
                StreamId id = new StreamId(stage.app, stage.apphost);
                Stream stream = streamIdMap.get(id);
                if (stream == null) {
                    LOG.error("can not find stream by streamid: " + id);
                    continue;
                }
                ArrayList<Stage> stages = Streams.get(stream);
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
    
    /*
     * cancel the key, remove it from appNodes or collectorNodes, then revisit streams
     * 1. appNode fail, mark the stream as inactive, mark all the stages as pending unless the uploading stage
     * 2. collectorNode fail, reassign collector if it is current stage, mark the stream as pending when no available collector;
     *  do recovery if the stage is not current stage, mark the stage as pending when no available collector
     */
    private void closeConnection(Connection connection) {
        if (!connection.isActive()) {
            LOG.info("connection " + connection + "already closed" );
            return;
        }
        
        SelectionKey key = connection.getChannel().keyFor(selector);
        LOG.info("close connection: " + connection);
        key.cancel();
        
        long now = Util.getTS();
        String host = connection.getHost();
        if (connection.getNodeType() == Connection.APPNODE) {
            appNodes.remove(host);
            LOG.info("close APPNODE: " + host);
            handleAppNodeFail(connection, now);
        } else if (connection.getNodeType() == Connection.COLLECTORNODE) {
            collectorNodes.remove(host);
            LOG.info("close COLLECTORNODE: " + host);
            handleCollectorNodeFail(connection, now);
        }
        
        connectionStreamMap.remove(connection);
        if (connection != null) {
            connection.close();
        }
    }

    private void dumpstat() {
        StringBuilder sb = new StringBuilder();
        sb.append("dumpstat:\n");
        sb.append("############################## dump ##############################\n");
        
        sb.append("print Streams:\n");
        for (Entry<Stream, ArrayList<Stage>> entry : Streams.entrySet()) {
            Stream stream = entry.getKey();
            sb.append("[stream]\n")
            .append(stream)
            .append("\n")
            .append("[stages]\n");
            ArrayList<Stage> stages = entry.getValue();
            synchronized (stages) {
                for (Stage stage : stages) {
                    sb.append(stage)
                    .append("\n");
                }
            }
        }
        sb.append("\n");
        
        sb.append("print streamIdMap:\n");
        for (Entry<StreamId, Stream> entry : streamIdMap.entrySet()) {
            sb.append("<")
            .append(entry.getKey())
            .append(", ")
            .append(entry.getValue())
            .append(">")
            .append("\n");
        }
        sb.append("\n");
        
        sb.append("print stageConnectionMap:\n");
        for(Entry<Stage, Connection> entry : stageConnectionMap.entrySet()) {
            sb.append("<")
            .append(entry.getKey())
            .append(", ")
            .append(entry.getValue())
            .append(">")
            .append("\n");
        }
        sb.append("\n");
        
        sb.append("print appNodes:\n");
        for(Entry<String, Connection> entry : appNodes.entrySet()) {
            sb.append("<")
            .append(entry.getKey())
            .append(", ")
            .append(entry.getValue())
            .append(">")
            .append("\n");
        }
        sb.append("\n");
        
        sb.append("print collectorNodes:\n");
        for(Entry<String, Connection> entry : collectorNodes.entrySet()) {
            sb.append("<")
            .append(entry.getKey())
            .append(", ")
            .append(entry.getValue())
            .append(">")
            .append("\n");
        }
        sb.append("\n");
        
        sb.append("print connectionStreamMap:\n");
        for(Entry<Connection, ArrayList<Stream>> entry : connectionStreamMap.entrySet()) {
            Connection conn = entry.getKey();
            sb.append(conn)
            .append("\n");
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
        
        LOG.info(sb.toString());
    }

    private void handleRetireStream(StreamID streamId) {
        StreamId id = new StreamId(streamId.getAppName(), streamId.getAppServer());
        Stream stream = streamIdMap.get(id);
        
        if (stream == null) {
            LOG.error("can't find stream by streamid: " + id);
            return;
        }
        
        if (stream.isActive()) {
            LOG.error("only inactive stream can be retired");
            return;
        } else {
            if (collectorNodes.isEmpty()) {
                LOG.error("only inactive app can be retired");
                return;
            }
            
            LOG.info("retire stream: " + stream);
            
            // remove from streamIdMap
            streamIdMap.remove(id);
            
            // remove the stages from stageConnectionMap
            ArrayList<Stage> stages = Streams.get(stream);
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
            for (Entry<Connection, ArrayList<Stream>> e : connectionStreamMap.entrySet()) {
                ArrayList<Stream> associatedStreams = e.getValue();
                synchronized (associatedStreams) {
                    associatedStreams.remove(stream);
                    if (associatedStreams.isEmpty()) {
                        connectionStreamMap.remove(e.getKey());
                    }
                }
            }
            
            // remove stream from Streams
            Streams.remove(stream);
        }
    }
    
    private void handleManualRecoveryRoll(RollID rollID) {
        StreamId id = new StreamId(rollID.getAppName(), rollID.getAppServer());
        Stream stream = streamIdMap.get(id);

        if (stream != null) {
            // check the stream is active
            if (!stream.isActive()) {
                LOG.error("the manual recovery stage must belong to an active stream");
                return;
            }
            ArrayList<Stage> stages = Streams.get(stream);
            if (stages != null) {
                synchronized (stages) {
                    // do not process exist stage
                    for (Stage stage : stages) {
                        if (stage.rollTs == rollID.getRollTs()) {
                            LOG.error("the manual recovery stage must not exist");
                            return;
                        }
                    }
                    // create the stage
                    Stage manualRecoveryStage = new Stage();
                    manualRecoveryStage.app = rollID.getAppName();
                    manualRecoveryStage.apphost = rollID.getAppServer();
                    manualRecoveryStage.collectorhost = null;
                    manualRecoveryStage.cleanstart = false;
                    manualRecoveryStage.issuelist = new ArrayList<Issue>();
                    manualRecoveryStage.status = Stage.RECOVERYING;
                    manualRecoveryStage.rollTs = rollID.getRollTs();
                    manualRecoveryStage.isCurrent = false;
                    // put the stage to head of the stages
                    ArrayList<Stage> newStages = new ArrayList<Stage>();
                    newStages.add(manualRecoveryStage);
                    newStages.addAll(newStages);
                    Streams.put(stream, newStages);
                    
                    // do recovery
                    doRecovery(stream, manualRecoveryStage);
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        } else {
            LOG.error("can't find stream by streamid: " + id);
        }   
    }

    private void handleUnrecoverable(RollID rollID) {
        StreamId id = new StreamId(rollID.getAppName(), rollID.getAppServer());
        Stream stream = streamIdMap.get(id);

        if (stream != null) {
            ArrayList<Stage> stages = Streams.get(stream);
            if (stages != null) {
                synchronized (stages) {
                    for (Stage stage : stages) {
                        if (stage.rollTs == rollID.getRollTs()) {
                            LOG.error("stage " + stage + " cannot be recovered");
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
            LOG.error("can't find stream by streamid: " + id);
        }
        
    }
    
    /*
     * failure happened only on stream
     * just log it in a issue, because:
     * if it is a collector fail, the corresponding log reader should find it 
     * and ask for a new collector; if it is a appnode fail, the appNode will
     * register the app again 
     */
    private void handleFailure(Failure failure) {
        StreamId id = new StreamId(failure.getApp(), failure.getAppServer());
        Stream stream = streamIdMap.get(id);
        
        if (stream == null) {
            LOG.error("can not find stream by streamid: " + id);
            return;
        }
        
        ArrayList<Stage> stages = Streams.get(stream);
        if (stages == null) {
            LOG.error("can not find stages of stream: " + stream);
            return;
        }
        
        synchronized (stages) {
            long failRollTs = Util.getRollTs(failure.getFailTs(), stream.period);
            Stage failstage = null;
            for (Stage s : stages) {
                if (s.rollTs == failRollTs) {
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
            if (failure.getType() == NodeType.APP_NODE) {
                Issue i = new Issue();
                i.ts = failure.getFailTs();
                i.desc = "logreader failed";
                failstage.issuelist.add(i);
            } else {
                Issue i = new Issue();
                i.ts = failure.getFailTs();
                i.desc = "collector failed";
                failstage.issuelist.add(i);
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
            if (stages != null) {
                synchronized (stages) {
                    for (Stage stage : stages) {
                        if (stage.rollTs == rollID.getRollTs()) {
                            doRecovery(stream, stage);
                            break;
                        }
                    }
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        } else {
            LOG.error("can't find stream by streamid: " + id);
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
            if (stages != null) {
                synchronized (stages) {
                    for (Stage stage : stages) {
                        if (stage.rollTs == rollID.getRollTs()) {
                            stage.status = Stage.UPLOADED;
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
            LOG.error("can't find stream by streamid: " + id);
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
            if (stages != null) {
                Issue e = new Issue();
                e.desc = "upload failed";
                e.ts = Util.getTS();
                synchronized (stages) {
                    for (Stage stage : stages) {
                        if (stage.rollTs == rollID.getRollTs()) {
                            stage.issuelist.add(e);
                            doRecovery(stream, stage);
                            break;
                        }
                    }
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        } else {
            LOG.error("can't find stream by streamid: " + id);
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
                            stageConnectionMap.remove(stage);
                            break;
                        }
                    }
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        } else {
            LOG.error("can't find stream by streamid: " + id);
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
        
        if (stream != null) {
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
                        LOG.error("Stage not found: " + stream + " rollTs " + msg.getRollTs());
                        return;
                    } else {
                        if (current.cleanstart == false || current.issuelist.size() != 0) {
                            current.isCurrent = false;
                            doRecovery(stream, current);
                        } else {
                            current.status = Stage.UPLOADING;
                            current.isCurrent = false;
                            doUpload(stream, current, from);
                        }
                    }
                    // create next stage if stream is connected
                    if (current.status != Stage.PENDING && current.status != Stage.COLLECTORFAIL) {
                        Stage next = new Stage();
                        next.app = stream.app;
                        next.apphost = stream.appHost;
                        next.collectorhost = current.collectorhost;
                        next.cleanstart = true;
                        next.rollTs = current.rollTs + stream.period * 1000;
                        next.status = Stage.APPENDING;
                        next.issuelist = new ArrayList<Issue>();
                        next.isCurrent = true;
                        
                        stages.add(next);
                    }
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        } else {
            LOG.error("can't find stream by streamid: " + id);
        }
    }

    private String doUpload(Stream stream, Stage current, Connection from) {
        Message message = PBwrap.wrapUploadRoll(current.app, current.apphost, stream.period, current.rollTs);
        send(from, message);
        stageConnectionMap.put(current, from);
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
            stageConnectionMap.remove(stage);
        } else {
            Connection c = appNodes.get(stream.appHost);
            Message message = PBwrap.wrapRecoveryRoll(stream.app, collector, stage.rollTs);
            send(c, message);
            
            stage.status = Stage.RECOVERYING;
            stage.collectorhost = collector;
            
            Connection collectorConnection = collectorNodes.get(collector);
            stageConnectionMap.put(stage, collectorConnection);
        }

        return collector;
    }
    
    /*
     * record the stream if it is a new stream;
     * do recovery if it is a old stream
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
            stream.setCollectorHost(message.getCollectorServer());
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
        } else {
            // old stream reconnected
            LOG.info("stream reconnected: " + stream);
            if (!stream.isActive()) {
                stream.updateActive(true);
            }
            
            // update collectorHost on stream
            stream.setCollectorHost(message.getCollectorServer());

            ArrayList<Stage> stages = Streams.get(stream);
            if (stages != null) {
                synchronized (stages) {
                    recoveryStages(stream, stages, connectedTs, currentTs, message.getCollectorServer());
                }
            } else {
                LOG.error("can not find stages of stream: " + stream);
            }
        }
        
        // register connection with appNode and collectorNode
        recordConnectionStreamMapping(from, stream);
        Connection appConnection = appNodes.get(stream.appHost);
        recordConnectionStreamMapping(appConnection, stream);
    }

    /*
     * 1. recovery appFail or collectFail stages (include current stage, but do no recovery)
     * 2. recovery missed stages (include current stage, but do no recovery)
     * 3. recovery current stage (realtime stream)with collector fail
     */
    private void recoveryStages(Stream stream, ArrayList<Stage> stages, long connectedTs, long currentTs, String newCollectorhost) {
        Issue issue = new Issue();
        issue.ts = connectedTs;
        issue.desc = "stream reconnected";
        
        // recovery Pending and COLLECTORFAIL stages
        for (int i=0 ; i < stages.size(); i++) {
            Stage stage = stages.get(i);
            LOG.info("processing pending stage: " + stage);
            if (stage.status == Stage.PENDING || stage.status == Stage.COLLECTORFAIL) {
                stage.issuelist.add(issue);
                // do not recovery current stage
                if (stage.rollTs != currentTs) {
                    doRecovery(stream, stage);
                } else {
                    // fix current stage status
                    stage.status = Stage.APPENDING;
                    stage.collectorhost = newCollectorhost;
                }
            }
        }

        // recovery missed stages
        int missedStageCount = getMissedStageCount(stream, connectedTs);
        LOG.info("need recovery possible missed stages: " + missedStageCount);

        if (stages.size() != 0) {
            Stage oldcurrent = stages.get(stages.size() - 1);
            if (oldcurrent.rollTs != currentTs) {
                oldcurrent.isCurrent = false;
            }
        } else {
            LOG.error("no stages found on stream: " + stream);
            return;
        }
        ArrayList<Stage> missedStages = getMissedStages(stream, missedStageCount, issue);
        for (Stage stage : missedStages) {
            LOG.info("processing possible missed stages: " + stage);
            // check whether it is missed
            if (!stages.contains(stage)) {
                LOG.info("process really missed stage: " + stage);
                // do not recovery current stage
                if (stage.rollTs != currentTs) {
                    doRecovery(stream, stage);
                } else {
                    stage.status = Stage.APPENDING;
                    stage.collectorhost = newCollectorhost;
                }
                stages.add(stage);
            } else {
                LOG.info("process not really missed stage: " + stage);
                int index = stages.indexOf(stage);
                Stage nmStage = stages.get(index);
                LOG.info("the not missed stage is " + nmStage);
                if (nmStage.rollTs != currentTs) {
                    nmStage.isCurrent = false;
                } else {
                    if (!nmStage.issuelist.contains(issue)) {
                        nmStage.issuelist.add(issue);
                    }
                }
            }
        }
    }
   
    private void recordConnectionStreamMapping(Connection connection, Stream stream) {
        if (connection == null) {
            LOG.fatal("Connection is null");
            return;
        }
        
        ArrayList<Stream> streams = connectionStreamMap.get(connection);
        if (streams == null) {
            streams = new ArrayList<Stream>();
            connectionStreamMap.put(connection, streams);
        }
        synchronized (streams) {
            if (!streams.contains(stream)) {
                streams.add(stream);
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
            message = PBwrap.wrapNoAvailableNode(app);
        }

        send(from, message);
        return collector;
    }
    
    private void registerCollectorNode(Connection from) {
        from.setNodeType(Connection.COLLECTORNODE);
        collectorNodes.put(from.getHost(), from);
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

        public Handler(int instanceNumber) {
            this.setDaemon(true);
            this.setName("process handler thread-"+instanceNumber);
        }
        
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
                handleFailure(msg.getFailure());
                break;
            case UNRECOVERABLE:
                handleUnrecoverable(msg.getRollID());
                break;
            case MANUAL_RECOVERY_ROLL:
                handleManualRecoveryRoll(msg.getRollID());
                break;
            case DUMPSTAT:
                dumpstat();
                break;
            case RETIRESTREAM:
                handleRetireStream(msg.getStreamId());
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
                    for (Connection c : connectionStreamMap.keySet()) {
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
       
    private void init() throws IOException, ClosedChannelException {
        Properties prop = new Properties();
        prop.load(new FileReader(new File("config.properties")));
        
        handlerCount = Integer.parseInt(prop.getProperty("supervisor.handlercount"));
        int port = Integer.parseInt(prop.getProperty("supervisor.port"));
        
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ServerSocket ss = ssc.socket();
        ss.bind(new InetSocketAddress(port));
        selector = Selector.open();
        ssc.register(selector, SelectionKey.OP_ACCEPT);
        connectionStreamMap = new ConcurrentHashMap<Connection, ArrayList<Stream>>();
        stageConnectionMap = new ConcurrentHashMap<Stage, Connection>();
        collectorNodes = new ConcurrentHashMap<String, Connection>();
        appNodes =  new ConcurrentHashMap<String, Connection>();
        messageQueue = new LinkedBlockingQueue<Msg>();
        Streams = new ConcurrentHashMap<Stream, ArrayList<Stage>>();
        streamIdMap = new ConcurrentHashMap<StreamId, Stream>();
        
        // start message handler thread
        handlers = new Handler[handlerCount];
        for (int i=0; i < handlerCount; i++) {
            handlers[i] = new Handler(i);
            handlers[i].start();
        }
        
        // start heart beat checker thread
        LiveChecker checker = new LiveChecker();
        checker.setDaemon(true);
        checker.start();
        
        LOG.info("supervisor started");
    }
    
    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        Supervisor supervisor = new Supervisor();
        supervisor.init();
        supervisor.loop();
    }

}

package com.dp.blackhole.supervisor;

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import com.dp.blackhole.common.gen.AppRegPB.AppReg;
import com.dp.blackhole.common.gen.AppRollPB.AppRoll;
import com.dp.blackhole.common.Connection;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.ReadyCollectorPB.ReadyCollector;
import com.dp.blackhole.common.gen.RollIDPB.RollID;
import com.dp.blackhole.common.Util;


public class Supervisor {

    public static final Log LOG = LogFactory.getLog(Supervisor.class);
    
    Selector selector;
    volatile private boolean running = true;
    
    private BlockingQueue<Msg> messageQueue;
    private ConcurrentHashMap<String, Connection> hostMap;
    private ConcurrentHashMap<Connection, ArrayList<StreamId>> connectionMap;
    private ArrayList<Connection> CollectorNodes;
    private ConcurrentHashMap<StreamId, ArrayList<Stage>> Streams;
    private int index;
    
    private class Msg {
        Message msg;
        Connection c;
    }
    
    private void send(Connection connection, Message msg) {
        SocketChannel channel = connection.getChannel();
        SelectionKey key = channel.keyFor(selector);
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        connection.offer(msg);
        LOG.debug("send message: "+msg);
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
                            ArrayList<StreamId> streams = new ArrayList<StreamId>();
                            connectionMap.put(connection, streams);
                        }
                        String remote = connection.getHost();
                        if (hostMap.get(remote) == null) {
                            hostMap.put(remote, connection);
                            LOG.debug("client node "+remote+" connected");
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
                        LOG.debug("receive message");
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
                            LOG.debug("message enqueue: " + msg);
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

    private void closeConnection(Connection connection) {
        SelectionKey key = connection.getChannel().keyFor(selector);
        LOG.info(key + "is closed, socketChannel is " + connection.getChannel());
        key.cancel();
        
        long now = Util.getTS();
        String host = connection.getHost();
        synchronized (CollectorNodes) {
            CollectorNodes.remove(connection);
        }
        hostMap.remove(host);
        ArrayList<StreamId> streams = connectionMap.get(connection);
        synchronized (streams) {
            for (StreamId stream : streams) {
                ArrayList<Stage> stages = Streams.get(stream);
                synchronized (stages) {
                    for (Stage stage : stages) {
                        // TODO handler status = uploading and stream may need to be marked as failed stream
                        if (stage.apphost.equals(host) && stage.status == Stage.APPEND) {
                            Issue e = new Issue();
                            e.desc = "logreader failed";
                            e.ts = now;
                            stage.issuelist.add(e);
                        }
                        if (stage.collectorhost.equals(host)) {
                            Issue e = new Issue();
                            e.desc = "collector failed";
                            stage.issuelist.add(e);
                            e.ts = now;
                            if (stage.isCurrent()) {
                                assignCollector(stage.app, hostMap.get(stage.apphost));
                            } else {
                                String newCollector = doRecovery(stream, stage);
                                stage.collectorhost = newCollector;
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
    
    private void init() throws IOException, ClosedChannelException {
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ServerSocket ss = ssc.socket();
        ss.bind(new InetSocketAddress(8080));
        selector = Selector.open();
        ssc.register(selector, SelectionKey.OP_ACCEPT);
        connectionMap = new ConcurrentHashMap<Connection, ArrayList<StreamId>>();
        hostMap = new ConcurrentHashMap<String, Connection>();
        CollectorNodes = new ArrayList<Connection>();
        messageQueue = new LinkedBlockingQueue<Msg>();
        Handler handler = new Handler();
        handler.start();
        LiveChecker checker = new LiveChecker();
        checker.start();
    }

    private void handleRecoveryFail(RollID rollID) {
        StreamId id = new StreamId();
        id.app = rollID.getAppName();
        id.appHost = rollID.getAppServer();
        ArrayList<Stage> stages = Streams.get(id);
        synchronized (stages) {
            for (Stage stage : stages) {
                if (stage.rollTs == rollID.getRollTs()) {
                    String newCollector = doRecovery(id, stage);
                    stage.collectorhost = newCollector;
                    break;
                }
            }
        }
    }

    private void handleRecoverySuccess(RollID rollID) {
        StreamId id = new StreamId();
        id.app = rollID.getAppName();
        id.appHost = rollID.getAppServer();
        ArrayList<Stage> stages = Streams.get(id);
        synchronized (stages) {
            for (Stage stage : stages) {
                if (stage.rollTs == rollID.getRollTs()) {
                    stage.status = Stage.UPLOAEDED;
                    LOG.info(stage.getSummary());
                    // TODO
                    stages.remove(stage);
                    break;
                }
            }
        }
    }

    private void handleUploadFail(RollID rollID) {
        StreamId id = new StreamId();
        id.app = rollID.getAppName();
        id.appHost = rollID.getAppServer();
        ArrayList<Stage> stages = Streams.get(id);
        Issue e = new Issue();
        e.desc = "upload failed";
        e.ts = Util.getTS();
        synchronized (stages) {
            for (Stage stage : stages) {
                if (stage.rollTs == rollID.getRollTs()) {
                    stage.issuelist.add(e);
                    String newCollector = doRecovery(id, stage);
                    stage.collectorhost = newCollector;
                    break;
                }
            }
        }
    }

    private String doRecovery(StreamId id, Stage stage) {
        String collector = getCollector();
        Connection c = hostMap.get(id.appHost);
        
        Message message = PBwrap.wrapRecoveryRoll(id.app, collector, stage.rollTs);
        send(c, message);
        return collector;
    }

    private void handleUploadSuccess(RollID rollID, Connection from) {
       StreamId id = new StreamId();
       id.app = rollID.getAppName();
       id.appHost = rollID.getAppServer();
       for (Entry<StreamId, ArrayList<Stage>> entry : Streams.entrySet()) {
           StreamId stream = entry.getKey();
           if (stream.equals(id)) {
               stream.lastSuccessTs = rollID.getRollTs();
               ArrayList<Stage> stages = entry.getValue();
               synchronized (stages) {
                   for (Stage stage : stages) {
                       if (stage.rollTs == rollID.getRollTs()) {
                           stage.status = Stage.UPLOAEDED;
                           LOG.info(stage.getSummary());
                           // TODO can not be removed here, historical not uploaded stage may exist
//                           stages.remove(stage);
                           break;
                       }
                   }
               }
               break;
           }
       }
    }

    private void handleRolling(AppRoll msg, Connection from) {
        StreamId stream = new StreamId();
        stream.app = msg.getAppName();
        stream.appHost = msg.getAppServer();
        ArrayList<Stage> stages = Streams.get(stream);
        if (stages != null) {
            synchronized (stages) {
                Stage current = stages.get(stages.size() - 1);
                if (current.cleanstart == false || current.issuelist.size() != 0) {
                    current.status = Stage.RECOVERYING;
                    current.isCurrent = false;
                    String newCollector = doRecovery(stream, current);
                    current.collectorhost = newCollector;
                } else {
                    current.status = Stage.UPLOADING;
                    current.isCurrent = false;
                    String uploadHost = doUpload(current, from);
                    current.collectorhost = uploadHost;
                }
                Stage next = new Stage();
                next.app = stream.app;
                next.apphost = stream.appHost;
                next.collectorhost = current.collectorhost;
                next.cleanstart = true;
                next.rollTs = getNextTs(stream, current.rollTs);
                next.status = Stage.APPEND;
                next.isCurrent = true;
                next.issuelist = new ArrayList<Issue>();
                stages.add(next);
            }
        }
    }

    private long getNextTs(StreamId stream, long rollTs) {
        // TODO Auto-generated method stub
        return rollTs + 3600 * 1000;
    }

    private String doUpload(Stage current, Connection from) {
        Message message = PBwrap.wrapUploadRoll(current.app, current.apphost, current.rollTs);
        send(from, message);
        return from.getHost();
    }

    private void registerCollector(ReadyCollector message, Connection from) {
        long now = Util.getTS();
        
        StreamId stream = new StreamId();
        stream.app = message.getAppName();
        stream.appHost = message.getAppName();
        stream.startTs = now;
        
        List<StreamId> streams = connectionMap.get(from);
        synchronized (streams) {
            streams.add(stream);
        }
        
        ArrayList<Stage> stages = Streams.get(stream);
        // new stream
        if (stages == null) {
            stages = new ArrayList<Stage>();
            Streams.put(stream, stages);
            Stage current = new Stage();
            current.apphost = stream.appHost;
            current.collectorhost = message.getCollectorServer();
            current.cleanstart = false;
            current.issuelist = new ArrayList<Issue>();
            current.status = Stage.APPEND;
            current.rollTs = Util.getRollTs(3600) + 3600;
            current.isCurrent = true;
            stages.add(current);
        } else {
            // old stream with collector fail
            Stage current = stages.get(stages.size() -1);
            if (message.getConnectedTs() < current.rollTs) {
                current.collectorhost = message.getCollectorServer();
            } else {
                String newCollector = doRecovery(stream, current);
                current.collectorhost = newCollector;
                current.isCurrent = false;
                Stage next = new Stage();
                next.apphost = stream.appHost;
                next.collectorhost = message.getCollectorServer();
                next.cleanstart = false;
                next.issuelist = new ArrayList<Issue>();
                next.status = Stage.APPEND;
                next.rollTs = Util.getRollTs(3600) + 3600;
                next.isCurrent = true;
                stages.add(next);
            }
        }

    }

    private void registerApp(Message m, Connection from) throws InterruptedException {
        AppReg message = m.getAppReg();
        int ret;
        ret = assignCollector(message.getAppName(), from);
        if (ret != 0) {
            Msg e = new Msg();
            e.msg = m;
            e.c = from;
            messageQueue.put(e);
        }
    }

    private int assignCollector(String app, Connection from) {
        if (CollectorNodes.isEmpty()) {
            return -1;
        }
        String collector = getCollector();
        Message message = PBwrap.wrapAssignCollector(app, collector);
        send(from, message);
        return 0;
    }

    private void registerCollectorNode(Connection from) {
        if (connectionMap.get(from) != null) {
            CollectorNodes.add(from);
        }
    }

    private String getCollector() {
        String collector;
        synchronized (CollectorNodes) {
            index = (++index) % CollectorNodes.size();
            collector = CollectorNodes.get(index).getHost();
        }
        return collector;
    }
    
    private class Handler extends Thread {

        @Override
        public void run() {
            while (true) {
                Msg e;
                try {
                    e = messageQueue.take();
                    process(e.msg, e.c);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
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
                registerCollector(msg.getReadyCollector(), from);
                break;
            case APP_ROLL:
                handleRolling(msg.getAppRoll(), from);
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
            default:
                LOG.warn("unknown message: " + msg.toString());
            }
        }
    }
    
    private class LiveChecker extends Thread {
        @Override
        public void run() {
            int THRESHOLD = 15 * 1000 * 1000;
            while (true) {
                try {
                    Thread.sleep(5000);
                    long now = Util.getTS();
                    for (Connection c : connectionMap.keySet()) {
                        if (now - c.getLastHeartBeat() > THRESHOLD) {
                            closeConnection(c);
                        }
                    }
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
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

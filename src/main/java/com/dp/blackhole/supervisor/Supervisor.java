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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import com.dp.blackhole.common.Connection;
import com.dp.blackhole.common.MessagePB;
import com.dp.blackhole.common.AssignCollectorPB.AssignCollector;
import com.dp.blackhole.common.MessagePB.Message;


public class Supervisor {

    public static final Log LOG = LogFactory.getLog(Supervisor.class);
    
    Selector selector;
    volatile private boolean running = true;
    
    private List<Connection> connectionList;
    private int numConnection = 0;
    
    private void writeMessage(Connection connection, Message msg) {
        SocketChannel channel = connection.getChannel();
        SelectionKey key = channel.keyFor(selector);
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        connection.offer(msg);
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

                        connectionList.add(connection);
                        numConnection++;
                        
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
                                closeConnection(key);
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
                            closeConnection(key);
                            break;
                        }
                        if (data.remaining() == 0) {
                            data.flip();
                            Message msg = Message.parseFrom(data.array());
                            process(msg, connection);
                            lengthBuf.clear();
                        }
                    }
                    key = null;
                }
            } catch (IOException e) {
                closeConnection(key);
            }
        }
    }

    private void closeConnection(SelectionKey key) {
        if (key != null) {
            Connection connection = (Connection) key.attachment();
            connectionList.remove(connection);
            numConnection--;
            LOG.info(key + "is closed, socketChannel is " + connection.getChannel());
            key.cancel();
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void init() throws IOException, ClosedChannelException {
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ServerSocket ss = ssc.socket();
        ss.bind(new InetSocketAddress(8080));
        selector = Selector.open();
        ssc.register(selector, SelectionKey.OP_ACCEPT);
        connectionList = new LinkedList<Connection>();
    }
    
    private void process(Message msg, Connection from) {
        switch (msg.getType()) {
        case APP_REG:
            writeMessage(from , getReply());
            break;
        default:
            LOG.warn("unknown message: " + msg.toString());
        }    
    }
    
    private Message getReply() {
        Message.Builder builder = MessagePB.Message.newBuilder();
        
        AssignCollector.Builder assignMessage = AssignCollector.newBuilder();
        assignMessage.setAppName("openAPI")
            .setCollectorServer("collector01")
            .setCollectorPort(20001);
        
        builder.setType(Message.MessageType.ASSIGN_COLLECTOR)
            .setAssignCollector(assignMessage.build());

        return builder.build();
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

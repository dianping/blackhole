package com.dp.blackhole.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GenClient<Entity, Connection extends NonblockingConnection<Entity>, Processor extends EntityProcessor<Entity, Connection>> {
    
    public static final Log LOG = LogFactory.getLog(GenClient.class);
    
    private ConnectionFactory<Connection> factory;
    private TypedFactory wrappedFactory;
    private Processor processor;
    
    private Selector selector;
    private SocketChannel socketChannel;
    volatile private boolean running = true;
    private ArrayList<Handler> handlers = null;
    private int handlerCount;
    private AtomicBoolean connected = new AtomicBoolean(false);
    private BlockingQueue<EntityEvent> entityQueue;

    private String host;
    private int port;
    
    private class EntityEvent {
        static final int CONNECTED = 1;
        static final int DISCONNECTED = 2;
        static final int RECEIVED = 3;
        int type;
        Entity entity;
        Connection c;
        public EntityEvent(int type, Entity entity, Connection c) {
            this.type = type;
            this.entity = entity;
            this.c = c;
        }
    }
    
    public GenClient(Processor processor, ConnectionFactory<Connection> factory, TypedFactory wrappedFactory) {
        this.processor = processor;
        this.factory = factory;
        this.wrappedFactory = wrappedFactory;
    }
    
    protected void loop() {
        while (running) {
            try {
                connect();
                loopInternal();
            } catch (ClosedChannelException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (running) {
                      Thread.sleep(3000);
                      LOG.info("reconnect in 3 second...");
                    }
                } catch (InterruptedException e) {
                }
            }
        }
    }
    
    protected void loopInternal() {
        SelectionKey key = null;
        while (running && socketChannel.isOpen()) {
            try {
                selector.select();
            } catch (IOException e) {
                LOG.error("IOException in select()", e);
                return;
            }
            Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
            while (iter.hasNext()) {
                key = iter.next();
                iter.remove();
                try {
                    if (key.isConnectable()) {
                        doConnect(key);
                    } else if (key.isWritable()) {
                        doWrite(key);
                    } else if (key.isReadable()) {
                        doRead(key);
                    }
                } catch (IOException e) {
                    LOG.warn("catch IOE: ", e);
                    closeConnection((Connection) key.attachment());
                }
            }
        }
    }

    private void doRead(SelectionKey key) throws IOException {
        Connection connection = (Connection) key.attachment();
        connection.read();
        
        if (connection.readComplete()) {
            Entity entity = connection.getEntity();
            entityQueue.add(new EntityEvent(EntityEvent.RECEIVED, entity, connection));
            connection.readyforRead();
        }
    }

    private void doWrite(SelectionKey key) throws IOException {
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        Connection connection = (Connection) key.attachment();
        
        connection.write();
        if (!connection.writeComplete()) {
            // socket buffer is full, register OP_WRITE, wait for next write
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private void doConnect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        key.interestOps(SelectionKey.OP_READ);
        channel.finishConnect();
        
        connected.getAndSet(true);
        Connection connection = factory.makeConnection(channel, selector, wrappedFactory);
        key.attach(connection);
        entityQueue.add(new EntityEvent(EntityEvent.CONNECTED, null, connection));
    }
    
    private void closeConnection(Connection connection) {
        if (connection == null) {
            return;
        }
        connected.getAndSet(false);
        if (!connection.isActive()) {
            LOG.info("connection " + connection + "already closed" );
            return;
        }
        
        entityQueue.add(new EntityEvent(EntityEvent.DISCONNECTED, null, connection));
        
        SelectionKey key = connection.getChannel().keyFor(selector);
        LOG.info("close connection: " + connection);
        key.attach(null);
        key.cancel();

        if (connection != null) {
            connection.close();
        }
    }

    public void shutdown() {
        running = false;
        selector.wakeup();
    }
    
    private class Handler extends Thread {

        public Handler(int instanceNumber) {
            this.setDaemon(true);
            this.setName("process handler thread-"+instanceNumber);
        }
        
        @Override
        public void run() {
            while (running) {
                EntityEvent e;
                try {
                    e = entityQueue.take();
                    switch (e.type) {
                    case EntityEvent.CONNECTED:
                        processor.OnConnected(e.c);
                        break;
                    case EntityEvent.DISCONNECTED:
                        processor.OnDisconnected(e.c);
                        break;
                    case EntityEvent.RECEIVED:
                        processor.process(e.entity, e.c);
                        break;
                    default:
                        LOG.error("unknow entity" + e);
                    }

                } catch (InterruptedException ie) {
                    LOG.info("handler thread interrupted");
                    running = false;
                }
            }
        }
    }
    
    public void init(Properties prop, String clientName, String serverHost, String serverPort) throws IOException, ClosedChannelException {  
        host = prop.getProperty(serverHost);
        port = Integer.parseInt(prop.getProperty(serverPort));;
        handlerCount = Integer.parseInt(prop.getProperty("GenClient.handlercount", "1"));
        
        entityQueue = new LinkedBlockingQueue<EntityEvent>();
        
        // start message handler thread
        handlers = new ArrayList<Handler>(handlerCount);
        for (int i=0; i < handlerCount; i++) {
            Handler handler = new Handler(i);
            handlers.add(handler);
            handler.start();
        }
        
        LOG.info("GenClient "+ clientName + " started");
        
        loop();
    }

    private void connect() throws IOException, ClosedChannelException {
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        SocketAddress server = new InetSocketAddress(host, port);
        socketChannel.connect(server);
        selector = Selector.open();
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
    }
}

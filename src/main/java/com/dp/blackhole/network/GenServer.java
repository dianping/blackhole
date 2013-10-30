package com.dp.blackhole.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GenServer<Entity, Connection extends NonblockingConnection<Entity>, Processor extends EntityProcessor<Entity, Connection>> {

    public static final Log LOG = LogFactory.getLog(GenServer.class);
    
    private ConnectionFactory<Connection> factory;
    private TypedFactory wrappedFactory;
    private Processor processor;
    
    private Selector selector;
    volatile private boolean running = true;
    private ArrayList<Handler> handlers = null;
    private int handlerCount = 1;
    
    private BlockingQueue<EntityEvent> entityQueue;
    
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
    
    public GenServer(Processor processor, ConnectionFactory<Connection> factory, TypedFactory wrappedFactory) {
        this.processor = processor;
        this.factory = factory;
        this.wrappedFactory = wrappedFactory;
    }
    
    protected void loop() {
        while (running) {
            SelectionKey key = null;
            try {
                selector.select();
            } catch (IOException e) {
                LOG.error("IOException in select()", e);
                running = false;
                continue;
            }
            Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
            while (iter.hasNext()) {
                key = iter.next();
                iter.remove();
                try {
                    if (key.isAcceptable()) {
                        doAccept(key);
                    } else if (key.isWritable()) {
                        doWrite(key);
                    } else if (key.isReadable()) {
                        doRead(key);
                    }
                } catch (IOException e) {
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
    
    private void doAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel channel = server.accept();
        if (channel == null) {
            throw new IOException("no connection is available to be accepted");
        }
        channel.configureBlocking(false);
        Connection connection = factory.makeConnection(channel, selector, wrappedFactory);
        entityQueue.add(new EntityEvent(EntityEvent.CONNECTED, null, connection));
        channel.register(selector, SelectionKey.OP_READ, connection);
    }
    
    private void closeConnection(Connection connection) {
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
       
    public void init(Properties prop, String name) throws IOException {
        handlerCount = Integer.parseInt(prop.getProperty("GenServer.handlercount"));
        int port = Integer.parseInt(prop.getProperty("GenServer.port"));
        
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ServerSocket ss = ssc.socket();
        ss.bind(new InetSocketAddress(port));
        selector = Selector.open();
        ssc.register(selector, SelectionKey.OP_ACCEPT);

        entityQueue = new LinkedBlockingQueue<EntityEvent>();

        // start message handler thread
        handlers = new ArrayList<Handler>(handlerCount);
        for (int i=0; i < handlerCount; i++) {
            Handler handler = new Handler(i);
            handlers.add(handler);
            handler.start();
        }
        
        LOG.info(name + " Server started");
        
        loop();
    }
}

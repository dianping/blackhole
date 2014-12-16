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
    private ServerSocketChannel serverSocketChannel;
    volatile private boolean running = true;
    private ArrayList<Handler> handlers = null;
    private int handlerCount;
    
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
                    if (key.isValid()) {
                        if (key.isAcceptable()) {
                            doAccept(key);
                        } else if (key.isWritable()) {
                            doWrite(key);
                        } else if (key.isReadable()) {
                            doRead(key);
                        }
                    }
                } catch (IOException e) {
                    LOG.warn("IOE catched: ", e);
                    closeConnection((Connection) key.attachment());
                }
            }
        }
        releaseResources();
    }
    
    private void releaseResources() {
        try {
            selector.close();
        } catch (Throwable t) {
            LOG.error(t.getMessage());
        }
        
        if (handlers != null) {
            for (Handler handler : handlers) {
                handler.interrupt();
            }
        }
        
        try {
            serverSocketChannel.close();
        } catch (Throwable t) {
            LOG.error(t.getMessage());
        }
    }

    private void doRead(SelectionKey key) throws IOException {
        Connection connection = (Connection) key.attachment();
        connection.read();
        
        if (connection.readComplete()) {
            Entity entity = connection.getEntity();
            Handler handler = getHandler(connection);
            handler.addEvent(new EntityEvent(EntityEvent.RECEIVED, entity, connection));
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
        SocketChannel channel;
        while ((channel = server.accept()) != null) {
            channel.configureBlocking(false);
            channel.socket().setTcpNoDelay(true);
            channel.socket().setKeepAlive(true);
            
            Connection connection = factory.makeConnection(channel, selector, wrappedFactory);
            Handler handler = getHandler(connection);
            handler.addEvent(new EntityEvent(EntityEvent.CONNECTED, null, connection));
            channel.register(selector, SelectionKey.OP_READ, connection);
        }
    }
    
    public void closeConnection(Connection connection) {
        if (connection == null) {
            return;
        }
        synchronized (connection) {
            if (!connection.isActive()) {
                LOG.info("connection " + connection + "already closed" );
                return;
            }
            SelectionKey key = connection.getChannel().keyFor(selector);
            
            LOG.info("close connection: " + connection);
            connection.close();
            
            key.attach(null);
            key.cancel();
            
            Handler handler = getHandler(connection);
            handler.addEvent(new EntityEvent(EntityEvent.DISCONNECTED, null, connection));
        }
    }

    public void shutdown() {
        running = false;
        if (selector != null) {
            selector.wakeup();
        }
    }
    
    private Handler getHandler(Connection connection) {
        int index = (connection.hashCode() & Integer.MAX_VALUE) % handlerCount;
        return handlers.get(index);
    }
    
    private class Handler extends Thread {
        private BlockingQueue<EntityEvent> entityQueue;

        public Handler(int instanceNumber) {
            entityQueue = new LinkedBlockingQueue<EntityEvent>();
            this.setDaemon(true);
            this.setName("process handler thread-"+instanceNumber);
        }
        
        public void addEvent(EntityEvent event) {
            entityQueue.add(event);
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
                } catch (Throwable t) {
                    LOG.error("exception catched when processing event", t);
                }
            }
        }
    }
       
    public void init(String name, int servicePort, int numHandler) throws IOException {
        handlerCount = numHandler;
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        ServerSocket ss = serverSocketChannel.socket();
        ss.bind(new InetSocketAddress(servicePort));
        selector = Selector.open();
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        
        // start message handler thread
        handlers = new ArrayList<Handler>(handlerCount);
        for (int i=0; i < handlerCount; i++) {
            Handler handler = new Handler(i);
            handlers.add(handler);
            handler.start();
        }
        
        LOG.info("GenServer " + name + " started at port:" + servicePort);
        
        loop();
    }
}

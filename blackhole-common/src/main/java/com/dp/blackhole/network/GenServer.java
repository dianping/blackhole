package com.dp.blackhole.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GenServer<Entity, Connection extends NonblockingConnection<Entity>, Processor extends EntityProcessor<Entity, Connection>> implements NioService<Entity, Connection> {

    public static final Log LOG = LogFactory.getLog(GenServer.class);
    
    private NonblockingConnectionFactory<Connection> factory;
    private TypedFactory wrappedFactory;
    private Processor processor;
    private ReceiveTimeoutWatcher<Entity, Connection> receiveTimeoutWatcher;
    
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    volatile private boolean running = true;
    private ArrayList<Handler<Entity, Connection>> handlers = null;
    private int handlerCount;
    

    
    public GenServer(Processor processor, NonblockingConnectionFactory<Connection> factory, TypedFactory wrappedFactory) {
        this.processor = processor;
        this.factory = factory;
        this.wrappedFactory = wrappedFactory;
    }
    
    protected void loop() {
        SelectionKey key = null;
        while (running) {
            try {
                selector.select();
            } catch (IOException e) {
                LOG.error("IOException in select()", e);
                running = false;
                continue;
            }
            try {
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
                        LOG.warn("IOE catched: " + e.getMessage());
                        closeConnection((Connection) key.attachment());
                    }
                }
            } catch (Exception e) {
                LOG.error("Oops, got an Exception", e);
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
            for (Handler<Entity, Connection> handler : handlers) {
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
            Handler<Entity, Connection> handler = getHandler(connection);
            handler.addEvent(new EntityEvent<Entity, Connection>(EntityEvent.RECEIVED, entity, connection));
            connection.readyforRead();
        }
    }

    private void doWrite(SelectionKey key) throws IOException {
        try {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        } catch (CancelledKeyException e) {
            LOG.warn("Exception while server writing." + e);
        }
        Connection connection = (Connection) key.attachment();
        
        connection.write();
        if (!connection.writeComplete()) {
            // socket buffer is full, register OP_WRITE, wait for next write
            try {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            } catch (CancelledKeyException e) {
                LOG.warn("Exception while server writing." + e);
            }
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
            channel.register(selector, SelectionKey.OP_READ, connection);
            Handler<Entity, Connection> handler = getHandler(connection);
            handler.addEvent(new EntityEvent<Entity, Connection>(EntityEvent.CONNECTED, null, connection));
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
            
            Handler<Entity, Connection> handler = getHandler(connection);
            handler.addEvent(new EntityEvent<Entity, Connection>(EntityEvent.DISCONNECTED, null, connection));
        }
    }

    public void send(Connection conn, Entity event) {
        if (conn.isActive()) {
            conn.send(event);
        } else {
            getHandler(conn).addEvent(new EntityEvent<Entity, Connection>(EntityEvent.SEND_FALURE, event, conn));
        }
    }

    public void sendWithExpect(String callId, Connection conn, Entity event, int expect, int timeout, TimeUnit unit) {
        if (conn.isActive()) {
            conn.send(event);
            receiveTimeoutWatcher.watch(callId, conn, event, expect, timeout, unit);
        } else {
            getHandler(conn).addEvent(new EntityEvent<Entity, Connection>(EntityEvent.SEND_FALURE, event, conn));
        }
    }
    
    public EntityEvent<Entity, Connection> unwatch(String callId, Connection conn, int expect) {
        return receiveTimeoutWatcher.unwatch(callId, conn, expect);
    }

    public void shutdown() {
        running = false;
        if (selector != null) {
            selector.wakeup();
        }
    }
    
    @Override
    public Handler<Entity, Connection> getHandler(Connection connection) {
        int index = (connection.hashCode() & Integer.MAX_VALUE) % handlerCount;
        return handlers.get(index);
    }
    
    @Override
    public boolean running() {
        return running;
    }

    @Override
    public Processor getProcessor() {
        return processor;
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
        handlers = new ArrayList<Handler<Entity, Connection>>(handlerCount);
        for (int i=0; i < handlerCount; i++) {
            Handler<Entity, Connection> handler = new Handler<Entity, Connection>(this, i);
            handlers.add(handler);
            handler.start();
        }

        receiveTimeoutWatcher = new ReceiveTimeoutWatcher<Entity, Connection>(this);
        processor.setNioService(this);
        
        LOG.info("GenServer " + name + " started at port:" + servicePort);
        
        loop();
    }
}

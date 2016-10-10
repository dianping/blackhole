package com.dp.blackhole.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GenClient<Entity, Connection extends NonblockingConnection<Entity>, Processor extends EntityProcessor<Entity, Connection>> implements NioService<Entity, Connection> {
    
    public static final Log LOG = LogFactory.getLog(GenClient.class);
    
    private NonblockingConnectionFactory<Connection> factory;
    private TypedFactory wrappedFactory;
    private Processor processor;
    private ReceiveTimeoutWatcher<Entity, Connection> receiveTimeoutWatcher;
    
    private Selector selector;
    private SocketChannel socketChannel;
    volatile private boolean running = true;
    private ArrayList<Handler<Entity, Connection>> handlers = null;
    private int handlerCount;
    private AtomicBoolean connected = new AtomicBoolean(false);

    private String clientName;
    private String host;
    private int port;
    
    public GenClient(Processor processor, NonblockingConnectionFactory<Connection> factory, TypedFactory wrappedFactory) {
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
                LOG.error("Channel cloesd", e);
            } catch (Exception e) {
                LOG.error("Oops, got an Exception", e);
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
                //interrupt the queue.take()
                handler.interrupt();
            }
        }
        
        try {
            socketChannel.close();
        } catch (Throwable t) {
            LOG.error(t.getMessage());
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
                    if (key.isValid()) {
                        if (key.isConnectable()) {
                            doConnect(key);
                        } else if (key.isWritable()) {
                            doWrite(key);
                        } else if (key.isReadable()) {
                            doRead(key);
                        }
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
            Handler<Entity, Connection> handler = getHandler(connection);
            handler.addEvent(new EntityEvent<Entity, Connection>(EntityEvent.RECEIVED, entity, connection));
            connection.readyforRead();
        }
    }

    private void doWrite(SelectionKey key) throws IOException {
        try {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        } catch (CancelledKeyException e) {
            LOG.warn("Exception while client writing." + e);
        }
        Connection connection = (Connection) key.attachment();
        
        connection.write();
        if (!connection.writeComplete()) {
            // socket buffer is full, register OP_WRITE, wait for next write
            try {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            } catch (CancelledKeyException e) {
                LOG.warn("Exception while client writing." + e);
            }
        }
    }

    private void doConnect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            key.interestOps(SelectionKey.OP_READ);
        } catch (CancelledKeyException e) {
            LOG.warn("Exception while client connecting." + e);
        }
        channel.finishConnect();
        LOG.info("GenClient "+ clientName + " connectted to " + host + ":" + port);
        
        connected.getAndSet(true);
        Connection connection = factory.makeConnection(channel, selector, wrappedFactory);
        key.attach(connection);
        Handler<Entity, Connection> handler = getHandler(connection);
        handler.addEvent(new EntityEvent<Entity, Connection>(EntityEvent.CONNECTED, null, connection));
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
        SelectionKey key = connection.getChannel().keyFor(selector);
        
        LOG.info("close connection: " + connection);
        connection.close();
        
        key.attach(null);
        key.cancel();
        
        Handler<Entity, Connection> handler = getHandler(connection);
        handler.addEvent(new EntityEvent<Entity, Connection>(EntityEvent.DISCONNECTED, null, connection));
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
    
//    private class Handler extends Thread {
//
//        public Handler(int instanceNumber) {
//            this.setDaemon(true);
//            this.setName("process handler thread@" + Integer.toHexString(hashCode()) + "-"+ instanceNumber );
//        }
//        
//        @Override
//        public void run() {
//            while (running) {
//                EntityEvent e;
//                try {
//                    e = entityQueue.take();
//                    switch (e.type) {
//                    case EntityEvent.CONNECTED:
//                        processor.OnConnected(e.c);
//                        break;
//                    case EntityEvent.DISCONNECTED:
//                        processor.OnDisconnected(e.c);
//                        break;
//                    case EntityEvent.RECEIVED:
//                        processor.process(e.entity, e.c);
//                        break;
//                    default:
//                        LOG.error("unknow entity" + e);
//                    }
//
//                } catch (InterruptedException ie) {
//                    LOG.info("handler thread interrupted");
//                    running = false;
//                } catch (Throwable t) {
//                    LOG.error("exception catched when processing event", t);
//                }
//            }
//        }
//    }

    @Override
    public boolean running() {
        return running;
    }

    @Override
    public Processor getProcessor() {
        return processor;
    }

    @Override
    public com.dp.blackhole.network.Handler<Entity, Connection> getHandler(Connection conn) {
        int index = (conn.hashCode() & Integer.MAX_VALUE) % handlerCount;
        return handlers.get(index);
    }
    
    public void init(String clientName, String serverHost, int serverPort) throws IOException, ClosedChannelException {  
        this.clientName = clientName;
        host = serverHost;
        port = serverPort;
        Properties prop = new Properties();
        handlerCount = Integer.parseInt(prop.getProperty("GenClient.handlercount", "1"));
        
        socketChannel = SocketChannel.open();
        selector = Selector.open();
        
        // start message handler thread
        handlers = new ArrayList<Handler<Entity, Connection>>(handlerCount);
        for (int i=0; i < handlerCount; i++) {
            Handler<Entity, Connection> handler = new Handler<Entity, Connection>(this, i);
            handlers.add(handler);
            handler.start();
        }
        
        receiveTimeoutWatcher = new ReceiveTimeoutWatcher<Entity, Connection>(this);
        processor.setNioService(this);
        
        loop();
    }

    private void connect() throws IOException, ClosedChannelException {
        if (socketChannel == null || !socketChannel.isOpen()) {
            socketChannel = SocketChannel.open();
        }
        socketChannel.configureBlocking(false);
        SocketAddress server = new InetSocketAddress(host, port);
        socketChannel.connect(server);
        LOG.info("GenClient "+ clientName + " connecting to " + host + ":" + port);
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
    }
}

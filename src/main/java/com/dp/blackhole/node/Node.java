package com.dp.blackhole.node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.MessagePB.Message.MessageType;

public abstract class Node {
    public static final Log LOG = LogFactory.getLog(Node.class);
    private Selector selector;
    private SocketChannel socketChannel;
    volatile private boolean running = true;
    private String host;
    private AtomicBoolean connected = new AtomicBoolean(false);
    
    private String supervisorhost;
    private int supervisorport;
    
    ByteBuffer readLength;
    ByteBuffer readbuffer;
    ByteBuffer writebuffer;
    
    private ConcurrentLinkedQueue<Message> queue; 
    
    class HeartBeat extends Thread {
        @Override
        public void run() {
            Message heartbeat = PBwrap.wrapHeartBeat();
            while (true) {
                try {
                    sleep(1000);
                    if (connected.get()) {
                        send(heartbeat);
                    }
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
    
    protected void loop() {
        while (running) {
            try {
                connectSupervisor();
                loopInternal();
            } catch (ClosedChannelException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                  Thread.sleep(3000);
                  LOG.info("reconnect in 3 second...");
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
    
    protected void loopInternal() {
        SelectionKey key = null;
        while (socketChannel.isOpen()) {
            try {
                selector.select();
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    key = iter.next();
                    iter.remove();
                    if (key.isConnectable()) {
                        SocketChannel channel = (SocketChannel) key.channel();
                        key.interestOps(SelectionKey.OP_READ);
                        channel.finishConnect();
                        connected.getAndSet(true);
                        onConnected();
                    } else if (key.isWritable()) {
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                        SocketChannel channel = (SocketChannel) key.channel();
                        while (true) {
                            if (writebuffer == null) {
                                Message msg = queue.peek();
                                if (msg == null) {
                                    break;
                                }
                                byte[] array = msg.toByteArray();
                                writebuffer = ByteBuffer.allocate(4 + array.length);
                                writebuffer.putInt(array.length);
                                writebuffer.put(array);
                                writebuffer.flip();
                            }
                            if (writebuffer.remaining() == 0) {
                                queue.poll();
                                writebuffer = null;
                                continue;
                            }
                            int num = -1;
                            for (int i = 0; i < 16; i++) {
                                num = channel.write(writebuffer);
                                if (num != 0) {
                                    break;
                                }
                            }
                            if (num == 0) {
                                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                                break;
                            }
                        }
                    } else if (key.isReadable()) {
                        SocketChannel channel = (SocketChannel) key.channel();

                        int count;

                        if (readLength.hasRemaining()) {
                            count = channel.read(readLength);
                            if (count < 0) {
                                closeconnection(key);
                                continue;
                            } else if (readLength.hasRemaining()) {
                                continue;
                            } else {
                                readLength.flip();
                                int length = readLength.getInt();
                                readbuffer = ByteBuffer.allocate(length);
                            }
                        }

                        count = channel.read(readbuffer);
                        if (count < 0) {
                            closeconnection(key);
                            continue;
                        }
                        if (readbuffer.remaining() == 0) {
                            readbuffer.flip();
                            Message msg = Message.parseFrom(readbuffer.array());
                            LOG.debug("message received: " + msg);
                            process(msg);
                            readbuffer = null;
                            readLength.clear();
                        }
                    }
                }
            } catch (IOException e) {
                LOG.warn("IOException catched: ", e);
                closeconnection(key);
            }
        }
    }

    protected void onConnected() {        
    }


    protected void onDisconnected() {
    }

    
    private void closeconnection(SelectionKey key) {
        connected.getAndSet(false);
        if (key != null) {
            SocketChannel channel = (SocketChannel) key.channel();
            LOG.debug("close connection: " + channel);
            key.cancel();
            readLength.clear();
            readbuffer = null;
            writebuffer = null;
            if (!channel.isOpen()) {
                return;
            }
            try {
                channel.socket().shutdownOutput();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                channel.socket().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        onDisconnected();
    }

    protected abstract boolean process(Message msg);

    protected void send(Message msg) {
        if (msg.getType() != MessageType.HEARTBEART) {
            LOG.debug("send message: " + msg);
        }
        queue.offer(msg);
        if (connected.get()) {
            SelectionKey key = socketChannel.keyFor(selector);
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    protected void init(String host, int port) throws IOException, ClosedChannelException {
        this.supervisorhost = host;
        this.supervisorport = port;
        
        readLength = ByteBuffer.allocate(4);
        this.queue = new ConcurrentLinkedQueue<Message>();
        this.host = Util.getLocalHost();
        
        HeartBeat heartBeatThread = new HeartBeat();
        heartBeatThread.setDaemon(true);
        heartBeatThread.start();
    }

    private void connectSupervisor() throws IOException, ClosedChannelException {
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        SocketAddress supervisor = new InetSocketAddress(supervisorhost, supervisorport);
        socketChannel.connect(supervisor);
        selector = Selector.open();
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
    }
    
    protected void clearMessageQueue () {
        queue.clear();
    }
    
    public String getHost() {
        return host;
    }
    
    public Node() {
        
    }
}


package com.dp.blackhole.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.MessagePB.Message.MessageType;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.MessagePB.Message;
import com.dp.blackhole.common.NodeRegPB.NodeReg;

public abstract class Node {
    public static final Log LOG = LogFactory.getLog(Node.class);
    private Selector selector;
    private SocketChannel socketChannel;
    volatile private boolean running = true;

    ByteBuffer readLength;
    ByteBuffer readbuffer;
    ByteBuffer writebuffer;
    
    private ConcurrentLinkedQueue<Message> queue; 
    
    class HeartBeat extends Thread {
        @Override
        public void run() {
            Message.Builder builder = Message.newBuilder();
            builder.setType(MessageType.HEARTBEART);
            Message heartbeat = builder.build();
            while (true) {
                try {
                    sleep(1000);
                    send(heartbeat);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
    
    protected void loop() {
        SelectionKey key = null;
        while (running) {
            try {
                selector.select(500);
                Iterator<SelectionKey> iter = selector.selectedKeys()
                        .iterator();
                while (iter.hasNext()) {
                    key = iter.next();
                    iter.remove();
                    if (key.isConnectable()) {
                        SocketChannel channel = (SocketChannel) key.channel();
                        key.interestOps(key.interestOps() | SelectionKey.OP_READ);
                        channel.finishConnect();
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
                                System.out.println(i);
                                System.out.println(writebuffer);
                                num = channel.write(writebuffer);
                                if (num != 0) {
                                    break;
                                }
                            }
                            if (num == 0) {
                                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                            }
                        }
                    } else if (key.isReadable()) {
                        LOG.debug("read data");
                        SocketChannel channel = (SocketChannel) key.channel();

                        int count;

                        if (readLength.hasRemaining()) {
                            count = channel.read(readLength);
                            if (count < 0) {
                                closeconnection(key);
                                running = false;
                                break;
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
                            break;
                        }
                        if (readbuffer.remaining() == 0) {
                            readbuffer.flip();
                            Message msg = Message.parseFrom(readbuffer.array());
                            process(msg);
                            readbuffer = null;
                            readLength.clear();
                        }
                    }
                }
            } catch (IOException e) {
                closeconnection(key);
            }
        }
    }

    private void closeconnection(SelectionKey key) {
        if (key != null) {
            SocketChannel channel = (SocketChannel) key.channel();
            key.cancel();
            readLength = null;
            readbuffer = null;
            writebuffer = null;
            if (!channel.isOpen()) {
                return;
            }
            try {
                channel.socket().shutdownOutput();
            } catch (IOException e1) {
                e1.printStackTrace();
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
    }

    protected abstract void process(Message msg);

    protected void send(Message msg) {
        queue.offer(msg);
        SelectionKey key = socketChannel.keyFor(selector);
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        selector.wakeup();
    }

    private void init() throws IOException, ClosedChannelException {
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        SocketAddress supervisor = new InetSocketAddress("localhost", 8080);    
        socketChannel.connect(supervisor);
        selector = Selector.open();
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
        readLength = ByteBuffer.allocate(4);
        this.queue = new ConcurrentLinkedQueue<Message>();
        
        HeartBeat heartBeatThread = new HeartBeat();
        heartBeatThread.setDaemon(true);
        heartBeatThread.start();
    }
}


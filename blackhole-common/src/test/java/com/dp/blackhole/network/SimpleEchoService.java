package com.dp.blackhole.network;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.junit.Test;

public class SimpleEchoService {

    public class Server extends Thread {
        class echoProcessor implements EntityProcessor<ByteBuffer, ByteBufferNonblockingConnection> {
            NioService<ByteBuffer, ByteBufferNonblockingConnection> service = null;
            
            @Override
            public void OnConnected(ByteBufferNonblockingConnection connection) {
                
            }

            @Override
            public void OnDisconnected(ByteBufferNonblockingConnection connection) {
                
            }

            @Override
            public void process(ByteBuffer request, ByteBufferNonblockingConnection conn) {
              service.send(conn, request);
            }

            @Override
            public void receiveTimout(ByteBuffer msg, ByteBufferNonblockingConnection from) {
                
            }

            @Override
            public void sendFailure(ByteBuffer msg, ByteBufferNonblockingConnection from) {
                
            }

            @Override
            public void setNioService(NioService<ByteBuffer, ByteBufferNonblockingConnection> service) {
                this.service = service;
                
            }
            
        }
        
        public void startService() throws IOException {
            GenServer<ByteBuffer, ByteBufferNonblockingConnection, echoProcessor> server = 
                    new GenServer(new echoProcessor(), new ByteBufferNonblockingConnection.ByteBufferNonblockingConnectionFactory(), null);
            server.init("echo", 2223, 1);
        }
        
        @Override
        public void run() {
            try {
                startService();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * @param args
     * @throws IOException 
     * @throws InterruptedException 
     */
    @Test
    public void testEcho() throws IOException, InterruptedException {
        SimpleEchoService echo = new SimpleEchoService();
        Server s = echo.new Server();
        s.setDaemon(true);
        s.start();
        
        Thread.sleep(1000);
        
        SocketAddress addr = new InetSocketAddress("localhost", 2223);
        SocketChannel channel = SocketChannel.open();
        channel.connect(addr);
        
        Charset charset = Charset.forName("UTF-8");
        CharsetEncoder encoder = charset.newEncoder();
        CharsetDecoder decoder = charset.newDecoder();
        
        ByteBuffer buf1 = encoder.encode(CharBuffer.wrap("123"));       
        ByteBuffer get1 = sendAndReceive(channel, buf1);
        String ret1 = decoder.decode(get1).toString();
        assertEquals("123", ret1);
        System.out.println(ret1);
        
        ByteBuffer buf2 = encoder.encode(CharBuffer.wrap("test"));       
        ByteBuffer get2 = sendAndReceive(channel, buf2);
        String ret2 = decoder.decode(get2).toString();
        assertEquals("test", ret2);
        
        ByteBuffer buf3 = encoder.encode(CharBuffer.wrap("another test"));       
        ByteBuffer get3 = sendAndReceive(channel, buf3);
        String ret3 = decoder.decode(get3).toString();
        assertEquals("another test", ret3);
    }

    private ByteBuffer sendAndReceive(SocketChannel channel, ByteBuffer buf)
            throws IOException {
        ByteBuffer send = ByteBuffer.allocate(4 + buf.limit());
        send.putInt(buf.limit());
        send.put(buf);
        send.flip();
        
        while (send.hasRemaining()) {
            channel.write(send);
        }
        
        ByteBuffer length = ByteBuffer.allocate(4);
        while (length.hasRemaining()) {
            channel.read(length);
        }
        length.flip();
        int len = length.getInt();
        ByteBuffer get = ByteBuffer.allocate(len);
        while (get.hasRemaining()) {
            channel.read(get);
        }
        get.flip();
        return get;
    }

}

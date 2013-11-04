package com.dp.blackhole.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Properties;

public class DelegationIOEchoService {
    
    class wappedObjectType implements TypedFactory {

        @Override
        public TypedWrappable getWrappedInstanceFromType(int type) {
            return new wappedObject();
        }
        
    }
    
    class wappedObject extends NonDelegationTypedWrappable {

        String data;
        
        public wappedObject() {
        }
        
        public wappedObject(String data) {
            this.data = data;
        }
        
        @Override
        public int getType() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int getSize() {
            // TODO Auto-generated method stub
            return data.getBytes().length;
        }

        @Override
        public void read(ByteBuffer contentBuffer) {
            data = new String(contentBuffer.array());
            
        }

        @Override
        public void write(ByteBuffer contentBuffer) {
            contentBuffer.put(data.getBytes());
            
        }
        
    }
    
    public class Server extends Thread {
        
        class echoProcessor implements EntityProcessor<TransferWrap, DelegationIOConnection> {

            @Override
            public void OnConnected(DelegationIOConnection connection) {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void OnDisconnected(DelegationIOConnection connection) {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void process(TransferWrap request, DelegationIOConnection from) {
                System.out.println("received "+((wappedObject)request.unwrap()).data);
                TransferWrap replay = new TransferWrap((wappedObject)request.unwrap());
                from.send(replay);
            }
            
        }
        
        private void startService() {
            GenServer<TransferWrap, DelegationIOConnection, EntityProcessor<TransferWrap, DelegationIOConnection>> server = new GenServer(
                    new echoProcessor(),
                    new DelegationIOConnection.DelegationIOConnectionFactory(),
                    new wappedObjectType());
            Properties prop = new Properties();
            prop.setProperty("supervisor.handlercount", "1");
            prop.setProperty("supervisor.port", "2222");
            try {
                server.init(prop, "echo");
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        @Override
        public void run() {
            startService();
        }

    }
    
    private wappedObject sendAndReceive(SocketChannel channel, wappedObject buf)
            throws IOException {
        
        TransferWrap sent = new TransferWrap(buf);
        
        while (!sent.complete()) {
            sent.write(channel);
        }
        
        TransferWrap receive = new TransferWrap(new wappedObjectType());
        
        while (!receive.complete()) {
            receive.read(channel);
        }

        return (wappedObject) receive.unwrap();
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
        DelegationIOEchoService echo = new DelegationIOEchoService();
        Server s = echo.new Server();
        s.setDaemon(true);
        s.start();
        
        Thread.sleep(1000);
        
        SocketAddress addr = new InetSocketAddress("localhost", 2222);
        SocketChannel channel = SocketChannel.open();
        channel.connect(addr);
        
        Charset charset = Charset.forName("UTF-8");
        CharsetEncoder encoder = charset.newEncoder();
        CharsetDecoder decoder = charset.newDecoder();
        
        wappedObject a = echo.sendAndReceive(channel, echo.new wappedObject("message123"));
        System.out.println("get reply "+a.data);
    }
}

package com.dp.blackhole.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
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
                System.out.println("server received request: "+((wappedObject)request.unwrap()).data);
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
            prop.setProperty("GenServer.handler.count", "1");
            prop.setProperty("GenServer.port", "2222");
            try {
                server.init(prop, "echo", "GenServer.port");
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
    
    class Client extends Thread {
        GenClient<TransferWrap, DelegationIOConnection, echoClientProcessor> client;
        private int echotimes;
        
        class echoClientProcessor implements EntityProcessor<TransferWrap, DelegationIOConnection> {

            @Override
            public void OnConnected(DelegationIOConnection connection) {
                DelegationIOEchoService echo1 = new DelegationIOEchoService();
                connection.send(new TransferWrap(echo1.new wappedObject("message123")));
                echotimes++;
            }

            @Override
            public void OnDisconnected(DelegationIOConnection connection) {
                
            }

            @Override
            public void process(TransferWrap request,
                    DelegationIOConnection from) {
                wappedObject a = (wappedObject) request.unwrap();
                System.out.println("client get reply: "+a.data);

                if (echotimes == 3) {
                    client.shutdown();
                    System.out.println();
                    System.out.println("total echo times: "+echotimes);
                } else {
                    TransferWrap echoRequest = new TransferWrap(a);
                    from.send(echoRequest);
                    echotimes++;
                }
            }
            
        }
        
        @Override
        public void run() {
            client = new GenClient(
                            new echoClientProcessor(),
                            new DelegationIOConnection.DelegationIOConnectionFactory(),
                            new wappedObjectType());
            Properties prop = new Properties();
            prop.setProperty("Server.host", "localhost");
            prop.setProperty("Server.port", "2222");
            try {
                client.init(prop, "echo", "Server.host", "Server.port");
            } catch (ClosedChannelException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
        DelegationIOEchoService echo = new DelegationIOEchoService();
        Server s = echo.new Server();
        s.setDaemon(true);
        s.start();
        
        Thread.sleep(1000);
        
        Client c = echo.new Client();
        c.setDaemon(true);
        c.start();
        c.join();
    }
}

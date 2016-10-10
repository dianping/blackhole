package com.dp.blackhole.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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
            return 0;
        }

        @Override
        public int getSize() {
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
        
        class echoProcessor implements EntityProcessor<TransferWrap, TransferWrapNonblockingConnection> {
            private NioService<TransferWrap, TransferWrapNonblockingConnection> service = null;

            @Override
            public void OnConnected(TransferWrapNonblockingConnection connection) {
                
            }

            @Override
            public void OnDisconnected(TransferWrapNonblockingConnection connection) {
                
            }

            @Override
            public void process(TransferWrap request, TransferWrapNonblockingConnection conn) {
                System.out.println("server received request: "+((wappedObject)request.unwrap()).data);
                TransferWrap replay = new TransferWrap((wappedObject)request.unwrap());
                
                int sleep = (new Random().nextInt(5));
                try {
                    System.out.println("Sleep Seconds: " + sleep);
                    Thread.sleep(sleep * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                service.send(conn, replay);
            }

            @Override
            public void receiveTimout(TransferWrap msg, TransferWrapNonblockingConnection from) {
                
            }

            @Override
            public void sendFailure(TransferWrap msg, TransferWrapNonblockingConnection from) {
                
            }

            @Override
            public void setNioService(NioService<TransferWrap, TransferWrapNonblockingConnection> service) {
                this.service = service;
                
            }
            
        }
        
        private void startService() {
            GenServer<TransferWrap, TransferWrapNonblockingConnection, EntityProcessor<TransferWrap, TransferWrapNonblockingConnection>> server = new GenServer(
                    new echoProcessor(),
                    new TransferWrapNonblockingConnection.TransferWrapNonblockingConnectionFactory(),
                    new wappedObjectType());
            try {
                server.init("echo", 2222, 1);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        @Override
        public void run() {
            startService();
        }

    }
    
    class Client extends Thread {
        GenClient<TransferWrap, TransferWrapNonblockingConnection, echoClientProcessor> client;
        private int echotimes;
        
        class echoClientProcessor implements EntityProcessor<TransferWrap, TransferWrapNonblockingConnection> {
            private NioService<TransferWrap, TransferWrapNonblockingConnection> service = null;

            @Override
            public void OnConnected(TransferWrapNonblockingConnection conn) {
                DelegationIOEchoService echo1 = new DelegationIOEchoService();
                service.send(conn, new TransferWrap(echo1.new wappedObject("message1")));
                echotimes++;
            }

            @Override
            public void OnDisconnected(TransferWrapNonblockingConnection conn) {
                
            }

            @Override
            public void process(TransferWrap request,
                    TransferWrapNonblockingConnection conn) {
                wappedObject a = (wappedObject) request.unwrap();
                System.out.println("client get reply: "+a.data);
                
                service.unwatch("CALLID_TEST", conn, 0);
                if (echotimes == 3) {
                    service.shutdown();
                    System.out.println();
                    System.out.println("total echo times: "+echotimes);
                } else {
                    TransferWrap echoRequest = new TransferWrap(a);
                    service.sendWithExpect("CALLID_TEST", conn, echoRequest, 0, 3, TimeUnit.SECONDS);
                    echotimes++;
                }
            }

            @Override
            public void receiveTimout(TransferWrap msg, TransferWrapNonblockingConnection conn) {
                System.out.println("### Timeout msg: " + ((wappedObject)msg.unwrap()).data);
            }

            @Override
            public void sendFailure(TransferWrap msg, TransferWrapNonblockingConnection conn) {
                
            }

            @Override
            public void setNioService(NioService<TransferWrap, TransferWrapNonblockingConnection> service) {
                this.service = service;
            }
            
        }
        
        @Override
        public void run() {
            client = new GenClient(
                            new echoClientProcessor(),
                            new TransferWrapNonblockingConnection.TransferWrapNonblockingConnectionFactory(),
                            new wappedObjectType());
            try {
                client.init("echo", "localhost", 2222);
            } catch (ClosedChannelException e) {
                e.printStackTrace();
            } catch (IOException e) {
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

package com.dp.blackhole.collectornode;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.dp.blackhole.collectornode.Publisher.PublisherExecutor;
import com.dp.blackhole.collectornode.persistent.ByteBufferMessageSet;
import com.dp.blackhole.collectornode.persistent.Message;
import com.dp.blackhole.collectornode.persistent.protocol.ProduceRequest;

public class Producer {
    
    private static String delimiter = "#";
    
    private Lock lock = new ReentrantLock();
    
    class preparedRequest {
        String topic;
        String partitionId;
        ByteBuffer buffer;
        
        public preparedRequest(String topic, String partitionId) {
            this.topic = topic;
            this.partitionId = partitionId;
            this.buffer = ByteBuffer.allocate(8 * 1024);
        }
        
        public void put(Message message) {
            message.write(buffer);
        }
        
        public int remaining() {
            return buffer.remaining();
        }
        
        public ProduceRequest createRequest() {
            buffer.flip();
            ByteBufferMessageSet messages = new ByteBufferMessageSet(buffer.slice());
            return new ProduceRequest(topic, partitionId, messages);
        }
    }
    
    private Map<String, preparedRequest> requests;
    PublisherExecutor p;
    
    public Producer() {
        requests = new ConcurrentHashMap<String, preparedRequest>();
        flush f = new flush();
        f.setDaemon(true);
        f.start();
    }
    
    public void setPublisher(PublisherExecutor p) {
        this.p = p;
    }
    
    private void send(ProduceRequest request) {
        p.handleProduceRequest(request, null);
    }
    
    public synchronized preparedRequest flush(String topic, String partitionId) {
        String id = topic + delimiter +partitionId;
        preparedRequest request = requests.get(id);
        send(request.createRequest());
        preparedRequest newRequest = new preparedRequest(topic, partitionId);
        requests.put(id, newRequest);
        return newRequest;
    }
    
    public void send(String topic, String partitionId, byte[] data) {
        lock.lock();
        try {
            String id = topic + delimiter +partitionId;
            preparedRequest request = requests.get(id);
            if (request == null) {
                request = new preparedRequest(topic, partitionId);
                requests.put(id, request);
            }
            Message message = new Message(data);
            if (message.getSize() > request.remaining()) {
                request = flush(topic, partitionId);
            }
            request.put(message);
        } finally {
            lock.unlock();
        }
    }
    
    class flush extends Thread {
        @Override
        public void run() {
           while (true) {
               try {
                Thread.sleep(100);
                lock.lock();
                try {
                for (Entry<String, preparedRequest> e : requests.entrySet()) {
                    String topic = e.getKey().split(delimiter)[0];
                    String partitionId = e.getKey().split(delimiter)[1];
                    flush(topic, partitionId);
                }
                } finally {
                    lock.unlock();
                }

            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
           }
        }
    }
}

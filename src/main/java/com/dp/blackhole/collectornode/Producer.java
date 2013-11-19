package com.dp.blackhole.collectornode.persistent;

import java.nio.ByteBuffer;
import java.util.HashMap;

import com.dp.blackhole.collectornode.Publisher;
import com.dp.blackhole.collectornode.persistent.protocol.ProduceRequest;

public class Producer {
    
    class preparedRequest {
        String topic;
        String partitionId;
        ByteBuffer buffer;
        
        public preparedRequest(String topic, String partitionId) {
            this.topic = topic;
            this.partitionId = partitionId;
            this.buffer = ByteBuffer.allocate(64 * 1024);
        }
        
        public void put(byte[] data) {
            buffer.put(data);
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
    
    private HashMap<String, preparedRequest> requests;
    Publisher p;
    
    public Producer() {
        requests = new HashMap<String, preparedRequest>();
    }
    
    public void setPublisher(Publisher p) {
        this.p = p;
    }
    
    private void send(ProduceRequest request) {
        p.handleProduceRequest(request, null);
    }
    
    public void send(String topic, String partitionId, byte[] data) {
        String id = topic + "-" + partitionId;
        preparedRequest request = requests.get(id);
        if (request == null) {
            request = new preparedRequest(topic, partitionId);
            requests.put(id, request);
        }
        if (data.length > request.remaining()) {
            send(request.createRequest());
            requests.put(id, new preparedRequest(topic, partitionId));
        }
        request.put(data);
    }
}

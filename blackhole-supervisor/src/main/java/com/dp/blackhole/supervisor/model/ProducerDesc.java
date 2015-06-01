package com.dp.blackhole.supervisor.model;

public class ProducerDesc extends NodeDesc{
    private final String topic;
    
    public ProducerDesc(String producerId, String topic) {
        super(producerId);
        this.topic = topic;
    }
    
    public String getTopic() {
        return topic;
    }
}

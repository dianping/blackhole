package com.dp.blackhole.network;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class Handler <Entity, Connection extends NonblockingConnection<Entity>> extends Thread {
    public static final Log LOG = LogFactory.getLog(Handler.class);
    
    private BlockingQueue<EntityEvent<Entity, Connection>> entityQueue;
    private NioService<Entity, Connection> service;

    public Handler(NioService<Entity, Connection> service, int instanceNumber) {
        this.service = service;
        this.entityQueue = new LinkedBlockingQueue<EntityEvent<Entity, Connection>>();
        this.setDaemon(true);
        this.setName("process handler thread-" + Integer.toHexString(hashCode()) + "-" + instanceNumber);
    }
    
    public void addEvent(EntityEvent<Entity, Connection> event) {
        entityQueue.add(event);
    }
    
    @Override
    public void run() {
        while (service.running()) {
            EntityEvent<Entity, Connection> e;
            try {
                e = entityQueue.take();
                synchronized(e.c) {
                    switch (e.type) {
                    case EntityEvent.CONNECTED:
                        service.getProcessor().OnConnected(e.c);
                        break;
                    case EntityEvent.DISCONNECTED:
                        service.getProcessor().OnDisconnected(e.c);
                        break;
                    case EntityEvent.RECEIVED:
                        service.getProcessor().process(e.entity, e.c);
                        break;
                    case EntityEvent.RECEIVE_TIMEOUT:
                        service.getProcessor().receiveTimout(e.entity, e.c);
                        break;
                    case EntityEvent.SEND_FALURE:
                        service.getProcessor().sendFailure(e.entity, e.c);
                        break;
                    default:
                        LOG.error("unknow entity" + e);
                    }
                }

            } catch (InterruptedException ie) {
                LOG.info("handler thread interrupted");
            } catch (Throwable t) {
                LOG.error("exception catched when processing event", t);
            }
        }
    }
}

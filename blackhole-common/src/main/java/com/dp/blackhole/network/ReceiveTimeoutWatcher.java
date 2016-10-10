package com.dp.blackhole.network;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.dp.blackhole.common.DaemonThreadFactory;

class ReceiveTimeoutWatcher<Entity, Connection extends NonblockingConnection<Entity>> {

    private NioService<Entity, Connection> service = null;
    private Map<String, EntityEvent<Entity, Connection>> watches;
    private ScheduledThreadPoolExecutor scheduler;

    private class TimeoutTask implements Runnable {
        private EntityEvent<Entity, Connection> e;

        public TimeoutTask(EntityEvent<Entity, Connection> e) {
            this.e = e;
        }

        @Override
        public void run() {
            synchronized(e.c) {
                if (unwatch(e.callId, e.c, e.expect) != null) {
                    service.getHandler(e.c).addEvent(e);
                }
            }
        }
    }

    public ReceiveTimeoutWatcher(NioService<Entity, Connection> service) {
        this.service = service;
        watches = Collections.synchronizedMap(new HashMap<String, EntityEvent<Entity, Connection>>());
        scheduler = new ScheduledThreadPoolExecutor(1, new DaemonThreadFactory("Scheduler"));
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    private String getKey(String callId, Connection conn, int expect) {
        return callId + conn + Integer.toString(expect);
    }

    void watch(String callId, Connection conn, Entity entity, int expect, int timeout, TimeUnit unit) {
        EntityEvent<Entity, Connection> v = new EntityEvent<Entity, Connection>(EntityEvent.RECEIVE_TIMEOUT, entity, expect, conn, callId);
        watches.put(getKey(callId, conn, expect), v);
        scheduler.schedule(new TimeoutTask(v), timeout, unit);
    }

    public EntityEvent<Entity, Connection> unwatch(String callId, Connection conn, int expect) {
        return watches.remove(getKey(callId, conn, expect));
    }
}

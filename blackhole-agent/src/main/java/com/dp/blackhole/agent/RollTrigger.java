package com.dp.blackhole.agent;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.DaemonThreadFactory;
import com.dp.blackhole.common.Util;

public class RollTrigger {
    private static final Log LOG = LogFactory.getLog(RollTrigger.class);
    private ScheduledExecutorService service ;
    private TopicMeta topicMeta;
    private LogFSM logFSM;

    public RollTrigger(TopicMeta topicMeta, LogFSM logFSM) {
        this.topicMeta = topicMeta;
        this.logFSM = logFSM;
        this.service = Executors
                .newSingleThreadScheduledExecutor(new DaemonThreadFactory(
                        "RollTrigger-" + topicMeta.getTopicId()));
    }
    
    public void trigger() {
        long currentTs = Util.getTS();
        long delay = Util.getNextRollTs(currentTs, topicMeta.getRollPeriod()) - currentTs + 100L;
        this.service.scheduleAtFixedRate(new RollAttemptTask(), delay, topicMeta.getRollPeriod() * 1000L, TimeUnit.MILLISECONDS);
        LOG.info("Roll Attempt will start after " + delay/1000L
                + " seconds, and repeat at fixed rate "
                + topicMeta.getRollPeriod() + " seconds");
    }

    public void stop() {
        service.shutdown();
        service = null;
    }
    
    class RollAttemptTask implements Runnable {
        @Override
        public void run() {
            long currentTs = System.currentTimeMillis();
            if (Util.isRollConcurrentWithRotate(currentTs, topicMeta.getRollPeriod(), topicMeta.getRotatePeriod())) {
                LOG.debug(topicMeta.getTopicId() + " roll attempt is not triggered cause rotating");
            } else {
                LOG.debug(topicMeta.getTopicId() + " begin roll attempt.");
                logFSM.beginRollAttempt();
            }
        }
    }
}

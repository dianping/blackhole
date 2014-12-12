package com.dp.blackhole.agent;

import java.util.Map;
import java.util.TimerTask;

import com.dp.blackhole.common.Util;

public class BatchTrigger extends TimerTask {

    @Override
    public void run() {
        for (Map.Entry<TopicMeta, LogReader> entry : Agent.getTopicReaders().entrySet()) {
            TopicMeta topicMeta = entry.getKey();
            LogReader logReader = entry.getValue();
            long currentTs = System.currentTimeMillis();
            long currentClosestStepTs = Util.getClosestStepTs(currentTs, topicMeta.getBatchPeriod());
            if (currentClosestStepTs % topicMeta.getRollPeriod() != 0) {
                //batch attempt is not triggered when log is rotating 
                logReader.beginBatchAttempt();
            }
        }
    }

}

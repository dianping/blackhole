package com.dp.blackhole.appnode;

import com.dianping.cat.Cat;
import com.dp.blackhole.common.Schedule;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ThroughputStat extends Schedule {
    private static final Log LOG = LogFactory.getLog(ThroughputStat.class);
    private Set<LogReader> logReaderSet = new HashSet<LogReader>();
    private long statPeriodSecond;

    public synchronized void add(LogReader logReader) {
        this.logReaderSet.add(logReader);
    }

    public synchronized void remove(LogReader logReader) {
        this.logReaderSet.remove(logReader);
    }

    public ThroughputStat(long sleepMillis) {
        super(sleepMillis);
        this.statPeriodSecond = (sleepMillis / 1000L);
    }

    public synchronized void action() {
        for (LogReader logReader : this.logReaderSet) {
            Cat.logMetricForDuration(logReader.getAppName(),
                    logReader.getReadSum() / this.statPeriodSecond);

            LOG.debug("REPORT [" + getClass().getSimpleName() + "] Key: "
                    + logReader.getAppName() + " Value: "
                    + logReader.getReadSum() / this.statPeriodSecond);

            logReader.resetReadSum();
        }
    }
}
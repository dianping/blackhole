package com.dp.blackhole.check;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

public class TimeChecker extends Thread {
    private final static Log LOG = LogFactory.getLog(TimeChecker.class);
    private boolean running = true;
    private long sleepDuration;
    private Map<RollIdent, List<Long>> checkerMap;

    public TimeChecker(long sleepDuration) {
        this.sleepDuration = sleepDuration;
        checkerMap = new HashMap<RollIdent, List<Long>>();
    }
    
    public synchronized void registerTimeChecker(RollIdent ident, long checkTs) {
        List<Long> checkTsList;
        if ((checkTsList = checkerMap.get(ident)) == null) {
            checkTsList = new ArrayList<Long>();
            checkerMap.put(ident, checkTsList);
        }
        checkTsList.add(checkTs);
    }
    
    public synchronized void unregisterTimeChecker(RollIdent ident, long checkTs) {
        List<Long> checkTsList;
        if ((checkTsList = checkerMap.get(ident)) != null) {
            checkTsList.remove(checkTs);
            if (checkTsList.isEmpty()) {
                checkerMap.remove(ident);
            }
        }
    }

    public void changePeriod(long newSleepDuration) {
        this.sleepDuration = newSleepDuration;
    }

    public void run() {
        while (this.running)
            try {
                check();
                Thread.sleep(this.sleepDuration);
            } catch (InterruptedException e) {
                this.running = false;
            }
    }

    public synchronized void check() {
        for (Map.Entry<RollIdent, List<Long>> entry : checkerMap.entrySet()) {
            RollIdent ident = entry.getKey();
            List<Long> checkTsList = entry.getValue();
            Path expectedFile = null;
            Path hiddenFile = null;
            for (int index = 0; index < checkTsList.size(); index++) {
                boolean shouldDone = true;
                for(String source : ident.sources) {
                    expectedFile = Util.getRollHdfsPathByTs(ident, checkTsList.get(index), source, false);
                    hiddenFile = Util.getRollHdfsPathByTs(ident, checkTsList.get(index), source, true);
                    if (Util.retryExists(expectedFile) || Util.retryExists(hiddenFile)) {
                    } else {
                        LOG.debug("TimeChecker: File " + expectedFile + " not ready.");
                        shouldDone = false;
                        break;
                    }
                }
                if (shouldDone) {
                    if (Util.retryTouch(expectedFile.getParent(), Util.DONE_FLAG)) {
                        LOG.info("TimeChecker: [" + ident.app + ":" + Util.format.format(new Date(checkTsList.get(index))) + "]....Done!");
                        checkTsList.remove(index);
                    } else {
                        LOG.error("TimeChecker: Alarm, failed to touch a DONE_FLAG file. " +
                                "Try in next check cycle. " +
                                "If you see this message for the second time, " +
                                "please find out why.");
                        break;
                    }
                }
            }
        }
    }
}
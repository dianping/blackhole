package com.dp.blackhole.check;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

public class TimeChecker extends Thread {
    private final static Log LOG = LogFactory.getLog(TimeChecker.class);
    private boolean running = true;
    private long sleepDuration;
    private LionConfChange lionConfChange;
    private Map<RollIdent, List<Long>> checkerMap;

    public TimeChecker(long sleepDuration, LionConfChange lionConfChange) {
        this.sleepDuration = sleepDuration;
        this.lionConfChange = lionConfChange;
        checkerMap = new ConcurrentHashMap<RollIdent, List<Long>>();
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
    
    public synchronized boolean isClear(RollIdent ident) {
        return !checkerMap.containsKey(ident);
    }

    public void changePeriod(long newSleepDuration) {
        this.sleepDuration = newSleepDuration;
    }

    public void run() {
        while (this.running) {
            try {
                check();
                Thread.sleep(this.sleepDuration);
            } catch (InterruptedException e) {
                this.running = false;
            } catch (Throwable t) {
                LOG.error("Catch an exception in TimeChecker", t);
            }
        }
    }

    public synchronized void check() {
        Iterator<Map.Entry<RollIdent, List<Long>>> iter =  checkerMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<RollIdent, List<Long>> entry = iter.next();
            RollIdent ident = entry.getKey();
            if (lionConfChange.getTopicBlacklist().contains(ident.topic)) {
                iter.remove();
                continue;
            }
            List<Long> checkTsList = entry.getValue();
            Path expectedFile[] = null;
            Path hiddenFile = null;
            for (int index = 0; index < checkTsList.size(); index++) {
                boolean shouldDone = true;
                long checkTs = checkTsList.get(index);
                Path parentPath = Util.getRollHdfsParentPath(ident, checkTs);
                Path missingDir = new Path(parentPath, CheckDone.missingSourcesDir);
                if (Util.wasDone(ident, checkTs)) {
                    unregisterTimeChecker(ident, checkTs);
                    if(!Util.retryDelete(missingDir)) {
                        LOG.error("Can not delete missing dir " + missingDir);
                    }
                    LOG.info("TimeChecker: [" + ident.topic + ":" + Util.format.format(new Date(checkTs)) + "]....Done!");
                    continue;
                }
                //skip source blacklist
                Set<String> shouldSkip = lionConfChange.getSkipSourceBlackList();
                ident.kvmSources.removeAll(shouldSkip);
                ident.paasSources.removeAll(shouldSkip);
                for(String source : ident.kvmSources) {
                    expectedFile = Util.getRollHdfsPathByTs(ident, checkTs, source, false);
                    hiddenFile = Util.getRollHdfsPathByTs(ident, checkTs, source, true)[0];
                    if (Util.retryExists(expectedFile) || Util.retryExists(hiddenFile)) {
                        //remove hidden sources file
                        if(!Util.retryDelete(missingDir, source)) {
                            LOG.error("Can not delete missing file " + missingDir + "/" + source);
                        }
                    } else {
                        shouldDone = false;
                        //add hidden sources file
                        if(!Util.retryTouch(missingDir, source)) {
                            LOG.error("Fail to touch missing source file" + missingDir + "/" + source);
                        }
                        LOG.debug("TimeChecker: None of " + Arrays.toString(expectedFile) + " is ready.");
                    }
                }
                for(String source : ident.paasSources) {
                    expectedFile = Util.getRollHdfsPathByTs(ident, checkTs, source, false);
                    hiddenFile = Util.getRollHdfsPathByTs(ident, checkTs, source, true)[0];
                    if (Util.retryExists(expectedFile) || Util.retryExists(hiddenFile)) {
                        //remove hidden paas sources file
                        if(!Util.retryDelete(missingDir, source)) {
                            LOG.error("Can not delete missing file " + missingDir + "/" + source);
                        }
                    } else {
                        shouldDone = false;
                        //add hidden paas sources file
                        if(!Util.retryTouch(missingDir, source)) {
                            LOG.error("Fail to touch missing source file" + missingDir + "/" + source);
                        }
                        LOG.debug("TimeChecker: None of " + Arrays.toString(expectedFile) + " is ready.");
                    }
                }
                
                if (shouldDone) {
                    if (Util.retryTouch(parentPath, CheckDone.doneFlag)) {
                        unregisterTimeChecker(ident, checkTsList.get(index));
                        if(!Util.retryDelete(missingDir)) {
                            LOG.error("Can not delete missing dir " + missingDir);
                        }
                        LOG.info("TimeChecker: [" + ident.topic + ":" + Util.format.format(new Date(checkTs)) + "]....Done!");
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

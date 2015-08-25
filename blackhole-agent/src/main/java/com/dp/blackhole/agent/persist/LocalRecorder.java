package com.dp.blackhole.agent.persist;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.agent.AgentMeta;
import com.dp.blackhole.agent.LogReader;
import com.dp.blackhole.agent.AgentMeta.TopicId;
import com.dp.blackhole.common.Util;

public class LocalRecorder implements IRecoder {
    private static final Log LOG = LogFactory.getLog(LocalRecorder.class);
    private AgentMeta topicMeta;
    private File snapshotFile;
    private Snapshot snapshot;
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock writeLock = lock.writeLock();
    private Lock readLock = lock.readLock();
    
    public LocalRecorder(String persistDir, AgentMeta meta) {
        this.topicMeta = meta;
        this.snapshotFile = getSnapshotFile(persistDir, meta.getTopicId());
        Snapshot snapshot;
        try {
            snapshot = reloadBySnapshotFile();
            setSnapshot(snapshot);
        } catch (Exception e) {
            LOG.info("Load '" + this.snapshotFile + "' faild, compute and build by skimming log files");
            snapshot = rebuidByActualFiles(meta, meta.getTopicId());
            setSnapshot(snapshot);
        }
    }

    private Snapshot reloadBySnapshotFile() throws IOException {
        return (Snapshot) Util.deserialize(FileUtils.readFileToByteArray(snapshotFile));
    }
    
    private Snapshot rebuidByActualFiles(final AgentMeta meta, TopicId id) {
        Snapshot snapshot = new Snapshot(id);
        final File file = new File(meta.getTailFile());
        if (file.exists()) {
            final SortedSet<String> sortTimeStrings = new TreeSet<String>();
            File parentDir = file.getParentFile();
            parentDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    String acceptFileNameRegex = file.getName() + "\\.(" + Util.getRegexFormPeriod(meta.getRotatePeriod()) + ")";
                    Pattern p = Pattern.compile(acceptFileNameRegex);
                    Matcher m = p.matcher(name);
                    if (m.find()) {
                        String timeString = m.group(1);
                        sortTimeStrings.add(timeString);
                        return true;
                    } else {
                        return false;
                    }
                }
            });
            for (String timeString : sortTimeStrings) {
                try {
                    long rotation = Util.parseTs(timeString, meta.getRotatePeriod());
                    snapshot.addRecord(new Record(Record.ROTATE, 
                            rotation + (meta.getRotatePeriod() - meta.getRollPeriod()) * 1000L,
                            LogReader.BEGIN_OFFSET_OF_FILE, LogReader.END_OFFSET_OF_FILE, rotation));
                } catch (ParseException e1) {
                    LOG.error("Time pases exception " + timeString);
                    continue;
                }
            }
            LOG.info("init-snapshot:\n" + snapshot);
        }
        return snapshot;
    }

    private File getSnapshotFile(String persistDir, TopicId id) {
        StringBuilder build = new StringBuilder();
        build.append(persistDir).append("/").append(id.getTopic()).append("/");
        if (id.getInstanceId() != null) {
            build.append(id.getInstanceId());
        } else {
            build.append(Util.getLocalHost());
        }
        build.append(".snapshot");
        return new File(build.toString());
    }
    
    public Snapshot getSnapshot() {
        readLock.lock();
        try {
            return snapshot;
        } finally {
            readLock.unlock();
        }
    }

    public void setSnapshot(Snapshot snapshot) {
        writeLock.lock();
        try {
            this.snapshot = snapshot;
        } finally {
            writeLock.unlock();
        }
    }
    
    @Override 
    public void record(int type, long rollTs, long startOffset, long endOffset, long currentRotation) {
        Record record = new Record(type, rollTs, startOffset, endOffset, currentRotation);
        getSnapshot().addRecord(record);
        LOG.info("Recorded " + record);
        try {
            persist();
        } catch (IOException e) {
            LOG.error("Persist " + record + " occur an IOException, remove it", e);
            getSnapshot().remove(record);;
        }
    }
    
    @Override
    public Record retrive(long rollTs) {
        Record ret = null;
        List<Record> records = getSnapshot().getRecords();
        if (!records.isEmpty()) {
            for (int i = records.size() - 1; i >= 0; i--) {
                Record record = records.get(i);
                if (record.getRollTs() == rollTs) {
                    ret = record;
                    break;
                }
            }
        }
        return ret;
    }
    
    @Override
    public Record retriveLastRecord(int type) {
        Record ret = null;
        List<Record> records = getSnapshot().getRecords();
        if (!records.isEmpty()) {
            for (int i = records.size() - 1; i >= 0; i--) {
                Record record = records.get(i);
                if (record.getType() == type) {
                    ret = record;
                    break;
                }
            }
        }
        return ret;
    }
    

    @Override
    public Record retriveFirstRecord() {
        Record ret = null;
        List<Record> records = getSnapshot().getRecords();
        if (!records.isEmpty()) {
            ret = records.get(0);
        }
        return ret;
    }

    @Override
    public void tidy() {
        Snapshot snapshot = rebuidByActualFiles(this.topicMeta, this.topicMeta.getTopicId());
        setSnapshot(snapshot);
        try {
            persist();
        } catch (IOException e) {
            LOG.error("Fail to persist snapshot after cleanup.", e);
        }
    }

    
    /**
     * clean up the snapshot file(delete force)
     * so it's a very dangerous opration.
     * please just invoke it before restart process
     */
    @Override
    public void cleanup() {
        snapshotFile.delete();
        snapshotFile = null;
    }

    @Override
    public boolean isComplete() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void log() {
        LOG.info(getSnapshot());
    }
    
    @Override
    public Record getPerviousRollRecord() {
        List<Record> records = getSnapshot().getRecords();
        Record perviousRollRecord = null;
        if (!records.isEmpty()) {
            perviousRollRecord = records.get(records.size() - 1);
        }
        return perviousRollRecord;
    }

    private void persist() throws IOException {
        getSnapshot().eliminateExpiredRecord();
        getSnapshot().setLastModifyTime(Util.getTS());
        byte[] toWrite = Util.serialize(getSnapshot());
        FileUtils.writeByteArrayToFile(snapshotFile, toWrite);
    }
}

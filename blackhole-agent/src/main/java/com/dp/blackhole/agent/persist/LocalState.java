package com.dp.blackhole.agent.persist;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.agent.LogReader;
import com.dp.blackhole.agent.TopicMeta;
import com.dp.blackhole.agent.TopicMeta.TopicId;
import com.dp.blackhole.common.Util;

public class LocalState implements IState {
    private static final Log LOG = LogFactory.getLog(LocalState.class);
    private File snapshotFile;
    private Snapshot snapshot;
    private final long rotatePeriod;
    
    public LocalState( String persistDir, TopicMeta meta) {
        this.rotatePeriod = meta.getLogRotatePeriod();
        TopicId id = meta.getTopicId();
        StringBuilder build = new StringBuilder();
        build.append(persistDir).append("/").append(id.getTopic()).append("/");
        if (id.getInstanceId() != null) {
            build.append(id.getInstanceId());
        } else {
            try {
                build.append(Util.getLocalHost());
            } catch (UnknownHostException e) {
                LOG.error("Oops, replace real hostname with 'localhost'", e);
                build.append("localhost");
            }
        }
        build.append(".snapshot");
        this.snapshotFile = new File(build.toString());
        try {
            this.snapshot = reload();
        } catch (IOException e) {
            LOG.info("File '" + this.snapshotFile + "' does not exist, compute and build by skim log files");
            this.snapshot = new Snapshot(id);
            final File file = new File(meta.getTailFile());
            if (file.exists()) {
                final SortedSet<String> sortTimeStrings = new TreeSet<String>();
                File parentDir = file.getParentFile();
                parentDir.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        String acceptFileNameRegex = file.getName() + "\\.(" + Util.getRegexFormPeriod(rotatePeriod) + ")";
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
                        this.snapshot.addRecord(new Record(Record.ROTATE, 
                                Util.parseTs(timeString, rotatePeriod)
                                + (meta.getLogRotatePeriod() - meta.getRollPeriod()) * 1000L,
                                LogReader.BOF, LogReader.EOF));
                    } catch (ParseException e1) {
                        LOG.error("Time pases exception " + timeString);
                        continue;
                    }
                }
            }
        }
    }
    
    public Snapshot getSnapshot() {
        return snapshot;
    }

    @Override
    public void record(int type, long rollTs, long endOffset) {
        long startOffset;
        Record perviousRollRecord = getPerviousRollRecord();
        if (perviousRollRecord == null) {
            startOffset = LogReader.BOF;
        } else if (!Util.belongToSameRotate(perviousRollRecord.getRollTs(), rollTs, rotatePeriod)) {
            startOffset = LogReader.BOF;
        } else {
            startOffset = perviousRollRecord.getEndOffset() + 1;
        }
        record(type, rollTs, startOffset, endOffset);
    }
    
    @Override 
    public void record(int type, long rollTs, long startOffset, long endOffset) {
        Record record = new Record(type, rollTs, startOffset, endOffset);
        snapshot.addRecord(record);
        LOG.debug("Recorded " + record);
        try {
            persist();
        } catch (IOException e) {
            LOG.error("Persist " + record + " occur an IOException, remove it", e);
            snapshot.remove(record);;
        }
    }
    
    @Override
    public Record retrive(long rollTs) {
        Record ret = null;
        List<Record> records = snapshot.getRecords();
        if (!records.isEmpty()) {
            for (int i = records.size() - 1; i >= 0; i--) {
                Record record = records.get(i);
                if (record.getRollTs() == rollTs) {
                    if (record.getType() == Record.ROLL
                            || record.getType() == Record.ROTATE) {
                        ret = record;
                        break;
                    } else if (record.getType() == Record.RESUME) {
                        if (i - 1 < 0) {
                            ret = record;
                        } else {
                            Record perviousRollRecord = records.get(i - 1);
                            if (perviousRollRecord.getRollTs() == record.getRollTs()) {
                                ret = perviousRollRecord;
                            } else {
                                ret = record;
                            }
                        }
                        break;
                    }
                }
            }
        }
        return ret;
    }
    
    public Record retriveLastRollRecord() {
        Record ret = null;
        List<Record> records = snapshot.getRecords();
        if (!records.isEmpty()) {
            for (int i = records.size() - 1; i >= 0; i--) {
                Record record = records.get(i);
                if (record.getType() == Record.ROLL
                        || record.getType() == Record.ROTATE) {
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
        List<Record> records = snapshot.getRecords();
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
        List<Record> records = snapshot.getRecords();
        if (!records.isEmpty()) {
            ret = records.get(0);
        }
        return ret;
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean isComplete() {
        // TODO Auto-generated method stub
        return false;
    }
    
    private Record getPerviousRollRecord() {
        List<Record> records = snapshot.getRecords();
        Record perviousRollRecord = null;
        if (!records.isEmpty()) {
            for (int i = records.size() - 1; i >= 0; i--) {
                Record record = records.get(i);
                if (record.getType() == Record.ROLL
                        || record.getType() == Record.ROTATE) {
                    perviousRollRecord = record;
                    break;
                } else if (record.getType() == Record.RESUME) {
                    if (i - 1 < 0) {
                        perviousRollRecord = record;
                    } else {
                        Record perviousPerviousRollRecord = records.get(i - 1);
                        if (perviousPerviousRollRecord.getRollTs() == record.getRollTs()) {
                            perviousRollRecord = perviousPerviousRollRecord;
                        } else {
                            perviousRollRecord = record;
                        }
                    }
                    break;
                }
            }
        }
        return perviousRollRecord;
    }

    private Snapshot reload() throws IOException {
        return (Snapshot) Util.deserialize(FileUtils.readFileToByteArray(snapshotFile));
    }

    private void persist() throws IOException {
        snapshot.eliminateExpiredRecord();
        snapshot.setLastModifyTime(Util.getTS());
        byte[] toWrite = Util.serialize(snapshot);
        FileUtils.writeByteArrayToFile(snapshotFile, toWrite);
    }
}

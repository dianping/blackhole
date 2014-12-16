package com.dp.blackhole.broker.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.broker.BrokerService;
import com.dp.blackhole.common.Util;

public class StorageManager {
    private final Log Log = LogFactory.getLog(StorageManager.class);
    
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Partition>> storage;
    
    private String basedir;
    private int flushThreshold;
    private int splitThreshold;
    
    public StorageManager(String basedir, int splitThreshold, int flushThreshold) throws IOException {
        this.basedir = basedir;
        this.splitThreshold = splitThreshold;
        this.flushThreshold = flushThreshold;
        storage = new ConcurrentHashMap<String, ConcurrentHashMap<String,Partition>>();
        
        // currently, clean storage when broker start for simplicity;
        // so load() has not effect
        Util.rmr(new File(basedir));
        load();
        
        reporter r = new reporter();
        r.setDaemon(true);
        r.start();
        
        cleanner c = new cleanner();
        c.setDaemon(true);
        c.start();
    }
    
    public void removePartition(String topic, String partitionId) {
        Map<String, Partition> map = storage.get(topic);
        if (map != null) {
            Partition p = map.get(partitionId);
            p.cleanupSegments(Util.getTS(), 0);
            map.remove(partitionId);
        }
    }
    
    private void load() throws IOException {
        File baseDir = new File(basedir);
        Util.checkDir(baseDir);
        File[] topicDirs = baseDir.listFiles();
        for (File topicDir : topicDirs) {
            if (!topicDir.isDirectory()) {
                continue;
            }
            File[] partitionDirs = topicDir.listFiles();
            String topic = topicDir.getName();
            ConcurrentHashMap<String, Partition> partitions = new ConcurrentHashMap<String, Partition>();
            storage.put(topic, partitions);
            for (File partitionDir : partitionDirs) {
                String partitionId = partitionDir.getName();
                Partition partition = new Partition(basedir, topic, partitionId, splitThreshold, flushThreshold);
                partitions.put(partitionId, partition);
            }
        }
        
    }

    public Partition getPartition(String topic, String partitionId, boolean createIfNonexist) throws IOException {
        // add new topic if not exist
        ConcurrentHashMap<String, Partition> map = storage.get(topic);
        if (map == null) {
            if (!createIfNonexist) {
                return null;
            }
            ConcurrentHashMap<String, Partition> newMap = new ConcurrentHashMap<String, Partition>();
            storage.putIfAbsent(topic, newMap);
            map = storage.get(topic);
        }
           
        // add new partition if not exist
        Partition partition = map.get(partitionId);
        if (partition == null) {
            if (!createIfNonexist) {
                return null;
            }
            Partition newPartition = new Partition(basedir, topic, partitionId, splitThreshold, flushThreshold);
            map.putIfAbsent(partitionId, newPartition);
            partition = map.get(partitionId);
        }
        return partition;
    }
    
    public int getFlushThreshold() {
        return flushThreshold;
    }
    
    public int getSplitThreshold() {
        return splitThreshold;
    }
    
    public class reporter extends Thread {
        
        public class ReportEntry {
            public final String topic;
            public final String partition;
            public final long offset;
            public ReportEntry(String topic, String partition, long offset) {
                this.topic = topic;
                this.partition = partition;
                this.offset = offset;
            }
        }
        
        private List<ReportEntry> entrylist;
        private Map<String, Long> reportedOffsets;
        private long interval = 3000;
        
        public reporter() {
            reportedOffsets = new HashMap<String, Long>();
        }
        
        private void report(String topic, String paritionId, long offset) {
            entrylist.add(new ReportEntry(topic, paritionId, offset));
        }
        
        private void report() {
            entrylist = new ArrayList<ReportEntry>();
            for (Entry<String, ConcurrentHashMap<String, Partition>> topicEntry : storage.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, Partition> partitions = topicEntry.getValue();
                for (Entry<String, Partition> partitionEntry : partitions.entrySet()) {
                    String partitionId = partitionEntry.getKey();
                    Partition p = partitionEntry.getValue();
                    long endOffset = p.getEndOffset();
                    String topicPartition = topic+"@"+partitionId;
                    Long reportedOffset = reportedOffsets.get(topicPartition);
                    if (reportedOffset != null) {
                        // report when new data arrive (endOffset > reportedOffset)
                        // or agent reconnecting to create a new segment file (endOffset < reportedOffset)
                        if (endOffset != reportedOffset) {
                            reportedOffsets.put(topicPartition, endOffset);
                            report(topic, partitionId, endOffset);
                        }
                    } else {
                        reportedOffsets.put(topicPartition, endOffset);
                        report(topic, partitionId, endOffset);
                    }
                }
            }
            if (entrylist.size() != 0) {
                BrokerService.reportPartitionInfo(entrylist);
            }
        }
        
        @Override
        public void run() {
            Log.info("start report thread at interval " + interval/1000);
            while (true) {
                try {
                    Thread.sleep(interval);
                    report();
                } catch (InterruptedException e) {
                }
                
            }
        }
    }
    
    public class cleanner extends Thread {
        private long threshold = 24 * 3600 * 1000l;
        private long interval = 3600 * 1000l;
        
        private void cleanup() {
            long current = Util.getTS();
            for (Map<String, Partition> m : storage.values()) {
                for (Partition p : m.values()) {
                    p.cleanupSegments(current, threshold);
                }
            }
        }
        
        @Override
        public void run() {
            Log.info("start cleanner thread at interval " + interval/1000);
            while (true) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    Log.error(e.getMessage());
                }
                cleanup();
            }
        }
                
    }
}

package com.dp.blackhole.collectornode.persistent;

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

import com.dp.blackhole.collectornode.BrokerService;
import com.dp.blackhole.common.Util;

public class PersistentManager {
    private final Log Log = LogFactory.getLog(PersistentManager.class);
    
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Partition>> storage;
    
    private String basedir;
    private int flushThreshold;
    private int splitThreshold;
    
    public PersistentManager(String basedir, int splitThreshold, int flushThreshold) throws IOException {
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

    public Partition getPartition(String topic, String partitionId) throws IOException {
        // add new topic if not exist
        ConcurrentHashMap<String, Partition> map = storage.get(topic);
        if (map == null) {
            ConcurrentHashMap<String, Partition> newMap = new ConcurrentHashMap<String, Partition>();
            storage.putIfAbsent(topic, newMap);
            map = storage.get(topic);
        }
           
        // add new partition if not exist
        Partition partition = map.get(partitionId);
        if (partition == null) {
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
        private Map<String, Long> knownTopicParitionOffsets;
        private long interval = 10000;
        
        public reporter() {
            knownTopicParitionOffsets = new HashMap<String, Long>();
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
                    long currentEndOffset = p.getEndOffset();
                    String topicPartition = topic+"@"+partitionId;
                    Long knownOffset = knownTopicParitionOffsets.get(topicPartition);
                    if (knownOffset != null) {
                        if (knownOffset < currentEndOffset) {
                            report(topic, partitionId, currentEndOffset);
                        }
                    } else {
                        knownTopicParitionOffsets.put(topicPartition, currentEndOffset);
                        report(topic, partitionId, currentEndOffset);
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
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                cleanup();
            }
        }
                
    }
}

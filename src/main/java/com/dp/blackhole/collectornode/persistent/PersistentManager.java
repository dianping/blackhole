package com.dp.blackhole.collectornode.persistent;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.dp.blackhole.collectornode.Publisher;
import com.dp.blackhole.common.Util;

public class PersistentManager {
    private Map<String, Map<String, Partition>> storage;
    
    private String basedir;
    private int flushThreshold;
    private int splitThreshold;
    
    public PersistentManager(String basedir, int splitThreshold, int flushThreshold) throws IOException {
        this.basedir = basedir;
        this.splitThreshold = splitThreshold;
        this.flushThreshold = flushThreshold;
        storage = new ConcurrentHashMap<String, Map<String,Partition>>();
        load();
        
        reporter r = new reporter();
        r.setDaemon(true);
        r.start();
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
        Map<String, Partition> map = storage.get(topic);
        if (map == null) {
            Partition partition = new Partition(basedir, topic, partitionId, splitThreshold, flushThreshold);
            map = new ConcurrentHashMap<String, Partition>();
            map.put(partitionId, partition);
            storage.put(topic, map);
            return partition;
        }
        // add new partition if not exist
        Partition partition = map.get(partitionId);
        if (partition == null) {
            partition = new Partition(basedir, topic, partitionId, splitThreshold, flushThreshold);
            map.put(topic, partition);
        }
        return partition;
        //TODO notify supervisor new topic/partition created
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
        public reporter() {
            knownTopicParitionOffsets = new HashMap<String, Long>();
        }
        
        private void report(String topic, String paritionId, long offset) {
            entrylist.add(new ReportEntry(topic, paritionId, offset));
        }
        
        private void report() {
            entrylist = new ArrayList();
            for (Entry<String, Map<String, Partition>> topicEntry : storage.entrySet()) {
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
            Publisher.reportPartitionInfo(entrylist);
        }
        
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(2000);
                    System.out.println("report");
                    report();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                
            }
        }
    }
    
}

package com.dp.blackhole.collectornode.persistent;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.dp.blackhole.common.Util;

public class PersistentManager {
    private Map<String, Map<String, Partition>> storage;
    
    private String basedir;
    private static int flushThreshold;
    private static int splitThreshold;
    
    public PersistentManager(String basedir, int splitThreshold, int flushThreshold) throws IOException {
        this.basedir = basedir;
        PersistentManager.splitThreshold = splitThreshold;
        PersistentManager.flushThreshold = flushThreshold;
        storage = new ConcurrentHashMap<String, Map<String,Partition>>();
        load();
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
                Partition partition = new Partition(basedir, topic, partitionId);
                partitions.put(partitionId, partition);
            }
        }
        
    }

    public Partition getPartition(String topic, String partitionId) throws IOException {
        // add new topic if not exist
        Map<String, Partition> map = storage.get(topic);
        if (map == null) {
            Partition partition = new Partition(basedir, topic, partitionId);
            map = new ConcurrentHashMap<String, Partition>();
            map.put(topic, partition);
            storage.put(topic, map);
            return partition;
        }
        // add new partition if not exist
        Partition partition = map.get(partitionId);
        if (partition == null) {
            partition = new Partition(basedir, topic, partitionId);
            map.put(topic, partition);
        }
        return partition;
        //TODO notify supervisor new topic/partition created
    }
    
    public static int getFlushThreshold() {
        return flushThreshold;
    }
    
    public static int getSplitThreshold() {
        return splitThreshold;
    }
    
}

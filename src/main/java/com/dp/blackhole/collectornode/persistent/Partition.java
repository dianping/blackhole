package com.dp.blackhole.collectornode.persistent;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.dp.blackhole.common.Util;

public class Partition {
    private String topic;
    private String id;
    private List<Segment> segments;
    private File dir;
    private final ReentrantReadWriteLock lock;
    
    int splitThreshold;
    int flushThreshold;
    
    public Partition(String basedir, String _topic, String _id, int splitThreshold, int flushThreshold) throws IOException {
        dir = new File(basedir + "/" + _topic + "/" + _id);
        topic = _topic;
        id = _id;
        segments = new ArrayList<Segment>();
        this.splitThreshold = splitThreshold;
        this.flushThreshold = flushThreshold;
        lock = new ReentrantReadWriteLock();
        loadSegments();
    }
    
    public String getId() {
        return id;
    }
    
    long getFileOffset(File f) {
        int dot = f.getName().lastIndexOf('.');   
        String offset = f.getName().substring(0, dot);
        return Long.parseLong(offset);
    }
    
    void loadSegments() throws IOException {
        Util.checkDir(dir);
        
        File[] segmentFiles = dir.listFiles(new FileFilter() {
            
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".blackhole");
            }
        });
        
        Arrays.sort(segmentFiles, new Comparator<File>() {

            @Override
            public int compare(File o1, File o2) {
                return (int) (getFileOffset(o1) - getFileOffset(o2));
            } 
        });
        
        for (int i = 0; i < segmentFiles.length; i++) {
            boolean verify;
            boolean readonly;
            if (i == segmentFiles.length-1) {
                verify = true;
                readonly = false;
            } else {
                verify = false;
                readonly = true;
            }
            Segment segment = new Segment(dir.getAbsolutePath(), getFileOffset(segmentFiles[i]), verify, readonly, splitThreshold, flushThreshold);
            segments.add(segment);
        }
    }
    
    private Segment addSegment(long offset) throws IOException {
        Segment segment = new Segment(dir.getAbsolutePath(), offset, false, false, splitThreshold, flushThreshold);
        lock.writeLock().lock();
        try {
            segments.add(segment);
        } finally {
            lock.writeLock().unlock();
        }
        return segment;
    }
    
    private Segment unprotectedGetLastSegment() {
        if (segments.size() == 0) {
            return null;
        }
        return segments.get(segments.size() - 1);
    }
    
    private Segment getLastSegment() {
        lock.readLock().lock();
        try {
            return unprotectedGetLastSegment();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public void append(MessageSet messages) throws IOException {
        Segment segment = unprotectedGetLastSegment();
        if (segment == null) {
            segment = addSegment(0);
        }
        long offset = segment.append(messages);
        if (offset != 0) {
            segment.flush();
            segment.setCloseTimestamp(Util.getTS());
            addSegment(offset);
        }
    }
    
    public long getEndOffset() {
        Segment segment = getLastSegment();
        if (segment == null) {
            return 0;
        } else {
            return segment.getEndOffset();
        }
    }
    
    public Segment findSegment(long offset) {
        lock.readLock().lock();
        try {
            if (segments.size() == 0) {
                return null;
            }
            int high = segments.size() -1;
            Segment last = segments.get(high);
            // TODO check last.getEndOffset() == offset condition
            if (last.contains(offset) || last.getEndOffset() == offset) {
                return last;
            } else if (last.getEndOffset() < offset) {
                return null;
            }
            
            int low = 0;
            Segment first = segments.get(0);
            if (first.contains(offset)) {
                return first;
            } else if (first.getStartOffset() > offset) {
                return null;
            }
            
            while (low < high) {
                int mid = (low + high)/2;
                Segment found = segments.get(mid);
                if (found.contains(offset)) {
                    return found;
                } else if (found.getStartOffset() > offset) {
                    high = mid -1;
                } else {
                    low = mid +1;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public FileMessageSet read(long offset, int length) {
        Segment segment = findSegment(offset);
        if (segment == null) {
            return null;
        }
        return segment.read(offset, length);
    }
    
    // for test only
    List<Segment> getSegments() {
        return segments;
    }
    
    public void cleanupSegments(long current, long threshold) {
        lock.writeLock().lock();
        try {
            for (Segment s : segments) {
                if (current - s.getCloseTimestamp() >= threshold) {
                    segments.remove(s);
                    s.destory();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}

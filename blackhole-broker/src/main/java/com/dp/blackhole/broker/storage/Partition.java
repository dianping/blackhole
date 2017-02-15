package com.dp.blackhole.broker.storage;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.storage.FileMessageSet;
import com.dp.blackhole.storage.MessageSet;

public class Partition {
    private final Log Log = LogFactory.getLog(Partition.class);
    
    private String topic;
    private String id;
    private List<Segment> segments;
    private File dir;
    private final ReentrantReadWriteLock lock;
    private RollPartition roll;
    private int entropy;
    
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
        roll = new RollPartition(this);
        loadSegments();
        this.entropy = 0;
    }
    
    public String getId() {
        return id;
    }
    
    public int getEntropy() {
        return this.entropy;
    }

    public boolean updateEntropy(int entropy) {
        this.entropy = entropy;
        return true;
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
                Long of1 = getFileOffset(o1);
                Long of2 = getFileOffset(o2);
                return of1.compareTo(of2);
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
        
        Segment s = unprotectedGetLastSegment();
        if (s == null) {
            roll.startOffset = 0;
        } else {
            roll.startOffset = s.getEndOffset();
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
    
    private Segment unprotectedGetFirstSegment() {
        if (segments.size() == 0) {
            return null;
        }
        return segments.get(0);
    }

    private Segment getFirstSegment() {
        lock.readLock().lock();
        try {
            return unprotectedGetFirstSegment();
        } finally {
            lock.readLock().unlock();
        }
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
        Segment segment = getLastSegment();
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
    
    public RollPartition markRollPartition() throws IOException {
        RollPartition ret = null;
        long endoffset;
        Segment segment = getLastSegment();
        if (segment == null) {
            throw new IOException("segment should not be null when mark rotate");
        }
        segment.flush();
        endoffset = segment.getEndOffset();
        
        roll.length = endoffset - roll.startOffset;
        ret = roll;
        roll = new RollPartition(this, endoffset);
        return ret;
    }

    public long getStartOffset() {
        Segment segment = getFirstSegment();
        if (segment == null) {
            return 0;
        } else {
            return segment.getStartOffset();
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
            
            while (low <= high) {
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
            Iterator<Segment> iter = segments.iterator();
            while(iter.hasNext()) {
                Segment s = iter.next();
                if (threshold == 0) {
                    s.setCloseTimestamp(current);
                }
                // the segment has not been closed (splitted)
                if (s.getCloseTimestamp() != 0 && current - s.getCloseTimestamp() >= threshold) {
                    Log.info("cleanup segment: " + s + " for " + id);
                    iter.remove();
                    s.destory();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void reInitSegment(long startOffset) throws IOException {
        addSegment(startOffset);
        this.roll.startOffset = startOffset;
    }

    public void flush() throws IOException {
        Segment segment = getLastSegment();
        if (segment == null) {
            return;
        }
        segment.flush();
    }

    public void truncate(long offset) throws IOException {
        try {
            lock.writeLock().lock();
            Segment s = this.getLastSegment();
            if (s == null) {
                throw new IOException("fail to find last segment");
            }
            if (s.getEndOffset() <= offset) {
                return;
            }
            s = findSegment(offset);
            if (s == null) {
                throw new IOException("fail to find segment for offset: " + offset);
            }
            int index = segments.indexOf(s);
            int size = segments.size();
            if (index != -1) {
                for (int i = size - 1; i > index; i--) {
                    Segment tmp = segments.get(i);
                    segments.remove(i);
                    tmp.destory();
                }
                s.channel.truncate(offset);
            }
        } catch (IOException e) {
            Log.error("fail to truncate segement topic: " + this.topic + ", partition: "
                    + this.id + ", offset: " + offset + "\n" + e);
            this.cleanupSegments(Util.getTS(), 0);
            this.reInitSegment(offset);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        return "Partition [topic=" + topic + ", id=" + id + ", dir=" + dir
                + ", splitThreshold=" + splitThreshold
                + ", flushThreshold=" + flushThreshold + "]";
    }
}

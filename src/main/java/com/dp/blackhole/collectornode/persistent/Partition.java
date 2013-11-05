package com.dp.blackhole.collectornode.persistent;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.dp.blackhole.common.Util;

public class Partition {
    String topic;
    String id;
    private List<Segment> segments;
    File dir;
    
    public Partition(String basedir, String _topic, String _id) throws IOException {
        dir = new File(basedir + "/" + _topic + "/" + _id);
        topic = _topic;
        id = _id;
        segments = new ArrayList<Segment>();
        loadSegments();
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
            Segment segment = new Segment(dir.getAbsolutePath(), getFileOffset(segmentFiles[i]), verify, readonly);
            segments.add(segment);
        }
    }
    
    private Segment addSegment(long offset) throws IOException {
        Segment segment = new Segment(dir.getAbsolutePath(), offset, false, false);
        segments.add(segment);
        return segment;
    }
    
    private Segment getLastSegment() {
        if (segments.size() == 0) {
            return null;
        }
        return segments.get(segments.size() - 1);
    }
    
    public void append(MessageSet messages) throws IOException {
        Segment segment = getLastSegment();
        if (segment == null) {
            segment = addSegment(0);
        }
        long offset = segment.append(messages);
        if (offset != 0) {
            segment.flush();
            addSegment(offset);
        }
    }
    
    Segment findSegment(long offset) {
        if (segments.size() == 0) {
            return null;
        }
        int high = segments.size() -1;
        Segment last = segments.get(high);
        if (last.contains(offset)) {
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
    }
    
    public FileMessageSet read(long offset, int length) {
        Segment segment = findSegment(offset);
        if (segment == null) {
            return null;
        }
        return segment.read(offset, length);
    }
}

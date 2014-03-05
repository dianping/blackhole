package com.dp.blackhole.collectornode;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dp.blackhole.collectornode.persistent.FileMessageSet;
import com.dp.blackhole.collectornode.persistent.Partition;
import com.dp.blackhole.collectornode.persistent.RollPartition;
import com.dp.blackhole.common.Util;

public class HDFSUpload_new implements Runnable{
    private static final Log LOG = LogFactory.getLog(HDFSUpload_new.class);
    private static final String TMP_SUFFIX = ".tmp";
    private RollManager mgr;
    private FileSystem fs;
    private DataOutputStream out;
    private RollIdent ident;
    private RollPartition roll;
    private boolean uploadSuccess;
    
    public HDFSUpload_new(RollManager mgr, FileSystem fs, RollIdent ident, RollPartition roll) {
        this.mgr = mgr;
        this.fs = fs;
        this.ident = ident;
        this.roll = roll;
        this.uploadSuccess = false;
    }
    
    @Override
    public void run() {
        
        mgr.reportUpload(ident, true);
        
//        String dfsPath = mgr.getRollHdfsPath(ident);
//        Path tmp = new Path(dfsPath + TMP_SUFFIX);
//        
//        Partition p = roll.p;
//        
//        long start = roll.startOffset;
//        int limit = (int) roll.length;
//        
//        FileMessageSet fmessages = p.read(start, limit); 
//        
//        try {
//            fs.copyFromLocalFile(false, true, src, tmp);
//            LOG.info("Collector file " + file + " has been uploaded.");
//            //rename
//            Path dst = new Path(dfsPath);
//            if (!Util.retryRename(fs, tmp, dst)) {
//                throw new IOException("Faild to rename tmp to " + dst);
//            }
//            uploadSuccess = true;
//        } catch (IOException e) {
//            LOG.error("Oops, got an exception:", e);
//        } finally {
//            mgr.reportUpload(ident, uploadSuccess);
//            try {
//                if (out != null) {
//                    out.close();
//                }
//            } catch (IOException e) {
//                LOG.warn("Faild to close Outputstream or RandomAccessFile ", e);
//            }
//        }
    }
}

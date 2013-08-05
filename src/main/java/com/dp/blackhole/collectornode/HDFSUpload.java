package com.dp.blackhole.collectornode;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dp.blackhole.common.Util;

public class HDFSUpload implements Runnable{
    private static final Log LOG = LogFactory.getLog(HDFSUpload.class);
    private static final String TMP_SUFFIX = ".tmp";
    private Collectornode node;
    private FileSystem fs;
    private File file;
    private DataOutputStream out;
    private RollIdent ident;
    private boolean uploadSuccess;
    
    public HDFSUpload(Collectornode node, FileSystem fs, File file, RollIdent roll) {
        this.node = node;
        this.fs = fs;
        this.file = file;
        this.ident = roll;
        this.uploadSuccess = false;
    }
    @Override
    public void run() {
        if (!file.isFile()) {
            LOG.error(file + "is not a FILE. Quite.");
            node.uploadResult(ident, uploadSuccess);
            return;
        }
        Path src = new Path(file.getPath());
        String dfsPath = node.getRollHdfsPath(ident);
        Path tmp = new Path(dfsPath + TMP_SUFFIX);
        
        try {
            fs.copyFromLocalFile(false, true, src, tmp);
            LOG.info("Collector file " + file + " has been uploaded.");
            //rename
            Path dst = new Path(dfsPath);
            if (!Util.retryRename(fs, tmp, dst)) {
                throw new Exception("Faild to rename tmp to " + dst);
            }
            uploadSuccess = true;
        } catch (Exception e) {
            LOG.error("Oops, got an exception:", e);
        } finally {
            node.uploadResult(ident, uploadSuccess);
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                LOG.warn("Faild to close Outputstream or RandomAccessFile ", e);
            }
        }
    }
}

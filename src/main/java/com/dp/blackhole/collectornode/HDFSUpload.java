package com.dp.blackhole.collectornode;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

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
    private boolean delsrc;
    private File file;
    private String dfsPath;
    private DataOutputStream out;
    private boolean uploadSuccess;
    
    public HDFSUpload(Collectornode node, FileSystem fs, File file, String dfsPath, boolean delsrc) {
        this.node = node;
        this.fs = fs;
        this.file = file;
        this.dfsPath = dfsPath;
        this.delsrc = delsrc;
        this.uploadSuccess = false;
    }
    @Override
    public void run() {
        if (!file.isFile()) {
            LOG.warn(file + "is not a FILE. Quite.");
            return;
        }
        Path src = new Path(file.getPath());
        Path tmp = new Path(dfsPath + TMP_SUFFIX);
        RandomAccessFile reader = null;
        
        try {
            fs.copyFromLocalFile(delsrc, true, src, tmp);
            LOG.info("Collector file " + file + " has been uploaded. " +
                		"Has deleted it? "+ delsrc);
            //rename
            Path dst = new Path(dfsPath);
            if (!Util.multipleRename(fs, tmp, dst)) {
                throw new Exception("Faild to rename tmp to " + dst);
            }
            uploadSuccess = true;
        } catch (Exception e) {
            LOG.error("Oops, got an exception:", e);
        } finally {
            node.uploadResult(this, uploadSuccess);
            try {
                if (out != null) {
                    out.close();
                }
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                LOG.warn("Faild to close Outputstream or RandomAccessFile ", e);
            }
        }
    }
}

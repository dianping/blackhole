package com.dp.blackhole.collectornode;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUpload implements Runnable{
    private static final Log LOG = LogFactory.getLog(HDFSUpload.class);
    private static final String TMP_SUFFIX = ".tmp";
    private Collectornode node;
    private FileSystem fs;
    private static final int DEFAULT_BUFSIZE = 8192;
    private File file;
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
            LOG.error(file + " is not a FILE. Quite.");
            node.uploadResult(ident, uploadSuccess);
            return;
        }
        String dfsPath = node.getRollHdfsPath(ident);
        Path tmp = new Path(dfsPath + TMP_SUFFIX);
        FSDataOutputStream out = null;
        FSDataInputStream in = null;
        int len = 0;
        byte[] buf = new byte[DEFAULT_BUFSIZE];
        try {
            in = fs.open(new Path(file.toURI()));
            out = fs.create(tmp);
            while((len = in.read(buf)) != -1) {
                out.write(buf, 0, len);
            }
            LOG.info("Collector file " + file + " has been uploaded.");
            out.close();
            out = null;
            //rename
            Path dst = new Path(dfsPath);
            if (!HDFSUtil.retryRename(fs, tmp, dst)) {
                throw new IOException("Faild to rename tmp to " + dst);
            }
            uploadSuccess = true;
        } catch (IOException e) {
            LOG.error("Oops, got an exception:", e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                    in = null;
                }
                if (out != null) {
                    out.close();
                    out = null;
                }
            } catch (IOException e) {
                LOG.warn("Faild to close Outputstream or RandomAccessFile ", e);
            }
            node.uploadResult(ident, uploadSuccess);
        }
    }
}

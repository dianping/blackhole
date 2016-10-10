package com.dp.blackhole.broker;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSMarker implements Runnable {
    private static final Log LOG = LogFactory.getLog(HDFSMarker.class);
    private RollManager mgr;
    private FileSystem fs;
    private RollIdent ident;
    
    public HDFSMarker(RollManager mgr, FileSystem fs, RollIdent ident) {
        this.mgr = mgr;
        this.fs = fs;
        this.ident = ident;
    }

    @Override
    public void run() {
        Path markFile = new Path(mgr.getMarkHdfsPath(ident));
        String tmpPathname = mgr.getTempHdfsPath(ident);
        String recoveryPathname = mgr.getRecoveryHdfsPath(ident);
        Path tmpPath = new Path(tmpPathname);
        Path recoveryPath = new Path(recoveryPathname);
        FSDataOutputStream out = null;
        try {
            if (fs.exists(tmpPath) && !HDFSUtil.retryDelete(fs, tmpPath)) {
                throw new IOException("could not recovery and faild to delete " + tmpPathname
                        + ".");
            }
            if (fs.exists(recoveryPath) && !HDFSUtil.retryDelete(fs, recoveryPath)) {
                throw new IOException("could not recovery and faild to delete " + recoveryPathname
                        + ".");
            }
            if (!fs.exists(markFile)) {
                out = fs.create(markFile);
            } else {
                LOG.info(markFile.getName() + " has existed.");
            }
        } catch (IOException e) {
            LOG.error("Failed to mark " + markFile.getName(), e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                }
                out = null;
            }
        }
    }
}

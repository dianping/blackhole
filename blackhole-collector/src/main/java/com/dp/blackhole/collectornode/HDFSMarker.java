package com.dp.blackhole.collectornode;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSMarker implements Runnable {
    private static final Log LOG = LogFactory.getLog(HDFSMarker.class);
    
    private Collectornode node;
    private FileSystem fs;
    private RollIdent ident;
    
    public HDFSMarker(Collectornode node, FileSystem fs, RollIdent ident) {
        this.node = node;
        this.fs = fs;
        this.ident = ident;
    }

    @Override
    public void run() {
        Path markFile = new Path(node.getMarkHdfsPath(ident));
        FSDataOutputStream out = null;
        try {
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

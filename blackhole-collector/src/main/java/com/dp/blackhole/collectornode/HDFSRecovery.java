package com.dp.blackhole.collectornode;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSRecovery implements Runnable{
    private static final Log LOG = LogFactory.getLog(HDFSUpload.class);
    private static final String TMP_SUFFIX = ".tmp";
    private static final String R_SUFFIX = ".r";

    private Collectornode node;
    private FileSystem fs;
    private static final int DEFAULT_BUFSIZE = 8192;
    private Socket client;
    private boolean recoverySuccess;
    private RollIdent ident;

    public HDFSRecovery(Collectornode node, FileSystem fs, Socket socket, RollIdent roll) {
        this.node = node;
        this.fs = fs;
        this.ident = roll;
        this.recoverySuccess = false;
        this.client = socket;
    }
    
    @Override
    public void run() {
        GZIPOutputStream gout = null;
        GZIPInputStream gin = null;
        try {
            String normalPathname = node.getRollHdfsPath(ident);
            String tmpPathname = normalPathname + TMP_SUFFIX;
            String recoveryPathname = normalPathname + R_SUFFIX;
            Path normalPath = new Path(normalPathname);
            Path tmpPath = new Path(tmpPathname);
            Path recoveryPath = new Path(recoveryPathname);
            if (!fs.exists(normalPath)) {   //When normal path not exists then do recovery.
                long offset = 0;
                int len = 0;
                byte[] buf = new byte[DEFAULT_BUFSIZE];
                gout = new GZIPOutputStream(fs.create(recoveryPath));
                if (fs.exists(tmpPath)) {   //When tmp path exists then extract some data from tmp file
                    gin = new GZIPInputStream(fs.open(tmpPath));
                    while((len = gin.read(buf)) != -1) {
                        gout.write(buf, 0, len);
                        offset += len;
                    }
                    LOG.info("Reloaded the hdfs file " + tmpPath);
                    
                    gin.close();
                    gin = null;
                }
                DataOutputStream out = new DataOutputStream(client.getOutputStream());
                out.writeLong(offset);
                LOG.info("Send an offset [" + offset + "] to client");
                
                BufferedInputStream in = new BufferedInputStream(client.getInputStream());
                while((len = in.read(buf)) != -1) {
                    gout.write(buf, 0, len);
                }
                LOG.info("Finished to write " + recoveryPath);
                
                gout.close();
                gout = null;
                
                if (Util.retryRename(fs, recoveryPath, normalPath)) {
                    LOG.info("Finished to rename to " + normalPathname);
                } else {
                    if (fs.exists(normalPath)) {
                        LOG.warn("failed to rename to " + normalPathname + ", it exists, delete " + recoveryPath);
                        if (!Util.retryDelete(fs, recoveryPath)) {
                            LOG.error("delete " + recoveryPath + " failed");
                        }
                    } else {
                        throw new IOException("Faild to rename to " + normalPathname);
                    }
                }
                
                if (fs.exists(tmpPath) && !Util.retryDelete(fs, tmpPath)) {   //delete .tmp if exists
                    throw new IOException("Faild to delete recovery file " + tmpPathname);
                }
            } else {    //When normal path exists then just delete .tmp if they exist.
                if (fs.exists(tmpPath) && !Util.retryDelete(fs, tmpPath)) {
                    throw new IOException("Faild to delete recovery file " + tmpPathname);
                }
            }
            recoverySuccess = true;
        } catch (IOException e) {
            LOG.error("Oops, got an exception:", e);
        } finally {
            node.recoveryResult(ident, recoverySuccess);
            try {
                if (gin != null) {
                    gin.close();
                }
                if (gout != null) {
                    gout.close();
                }
            } catch (IOException e) {
                LOG.warn("Cound not close the gzip output stream", e);
            }
        }
    }
}

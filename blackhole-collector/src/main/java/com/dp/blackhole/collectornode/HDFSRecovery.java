package com.dp.blackhole.collectornode;

import java.io.DataInputStream;
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
                LOG.debug("Begin to recovery " + normalPathname);
                long offset = 0;
                int len = 0;
                byte[] buf = new byte[DEFAULT_BUFSIZE];
                gout = new GZIPOutputStream(fs.create(recoveryPath));
                if (fs.exists(tmpPath)) {   //When tmp path exists then extract some data from tmp file
                    boolean tmpCanRead = true;
                    try {
                        gin = new GZIPInputStream(fs.open(tmpPath));
                    } catch (IOException e) {
                        LOG.error("Oops, can not open the tmp file, try to delete it.", e);
                        HDFSUtil.retryDelete(fs, tmpPath);
                        tmpCanRead = false;
                    }
                    while(tmpCanRead && (len = gin.read(buf)) != -1) {
                        gout.write(buf, 0, len);
                        offset += len;
                    }
                    if (tmpCanRead) {
                        LOG.info("Reloaded tmp hdfs file " + tmpPath);
                    } else {
                        LOG.info("Skipped reloading of tmp hdfs file " + tmpPath);
                    }
                }
                DataOutputStream out = new DataOutputStream(client.getOutputStream());
                out.writeLong(offset);
                LOG.info("Send an offset [" + offset + "] to client");
                
                DataInputStream in = new DataInputStream(client.getInputStream());
                while((len = in.read(buf)) != -1) {
                    gout.write(buf, 0, len);
                }
                LOG.info("Finished to write " + recoveryPath);
                
                gout.close();
                gout = null;
                
                if (HDFSUtil.retryRename(fs, recoveryPath, normalPath)) {
                    LOG.info("Finished to rename to " + normalPathname);
                } else {
                    if (!HDFSUtil.retryDelete(fs, recoveryPath)) {
                        LOG.error("delete " + recoveryPath + " failed, try next time.");
                    }
                    throw new IOException("Faild to rename to " + normalPathname);
                }
                
                if (fs.exists(tmpPath) && !HDFSUtil.retryDelete(fs, tmpPath)) {
                    throw new IOException("Recovery success but faild to delete " + tmpPathname
                            + ", it will try again next time.");
                }
            } else {
                LOG.info("File exists, do not need to repair.");
                //When normal path exists then just delete .tmp and .r if it exists.
                if (fs.exists(tmpPath) && !HDFSUtil.retryDelete(fs, tmpPath)) {
                    throw new IOException("Recovery success but faild to delete " + tmpPathname
                            + ", it will try again next time.");
                }
                if (fs.exists(recoveryPath) && !HDFSUtil.retryDelete(fs, recoveryPath)) {
                    throw new IOException("Recovery success but faild to delete " + recoveryPathname
                            + ", it will try again next time.");
                }
            }
            recoverySuccess = true;
        } catch (IOException e) {
            LOG.error("Oops, got an exception:", e);
        } finally {
            try {
                if (gin != null) {
                    gin.close();
                    gin = null;
                }
                if (gout != null) {
                    gout.close();
                    gout = null;
                }
                if (client != null && !client.isClosed()) {
                    client.close();
                    client = null;
                }
            } catch (IOException e) {
                LOG.warn("Cound not close the gzip output stream", e);
            }
            if (!recoverySuccess) {
                try {
                    Thread.sleep(3 * 1000);
                } catch (InterruptedException e) {}
            }
            node.recoveryResult(ident, recoverySuccess);
        }
    }
}

package com.dp.blackhole.broker;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSRecovery implements Runnable{
    private static final Log LOG = LogFactory.getLog(HDFSRecovery.class);
    private static final String TMP_SUFFIX = ".tmp";
    private static final String R_SUFFIX = ".r";

    private RollManager mgr;
    private FileSystem fs;
    private static final int DEFAULT_BUFSIZE = 8192;
    private Socket client;
    private boolean recoverySuccess;
    private RollIdent ident;
    private long fileSize;
    private boolean hasCompressed;

    public HDFSRecovery(RollManager mgr, FileSystem fs, Socket socket, RollIdent roll, long fileSize, boolean hasCompressed) {
        this.mgr = mgr;
        this.fs = fs;
        this.ident = roll;
        this.recoverySuccess = false;
        this.client = socket;
        this.fileSize = fileSize;
        this.hasCompressed = hasCompressed;
    }
    
    @Override
    public void run() {
        OutputStream out = null;
        try {
            String normalPathname = mgr.getRollHdfsPath(ident);
            String tmpPathname = normalPathname + TMP_SUFFIX;
            String recoveryPathname = normalPathname + R_SUFFIX;
            Path normalPath = new Path(normalPathname);
            Path tmpPath = new Path(tmpPathname);
            Path recoveryPath = new Path(recoveryPathname);
            if (!fs.exists(normalPath)) {   //When normal path not exists then do recovery.
                LOG.debug("Begin to recovery " + normalPathname);
                int len = 0;
                int uploadSize = 0;
                byte[] buf = new byte[DEFAULT_BUFSIZE];
                if (hasCompressed) {
                    out = fs.create(recoveryPath);
                } else {
                    out = new GZIPOutputStream(fs.create(recoveryPath));
                }
                DataInputStream in = new DataInputStream(client.getInputStream());
                while((len = in.read(buf)) != -1) {
                    out.write(buf, 0, len);
                    uploadSize += len;
                }
                out.close();
                out = null;
                if (uploadSize != fileSize) {
                    throw new IOException(recoveryPathname + " recoverying not finished.");
                }
                LOG.info("Finished to write " + recoveryPath);
                
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
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    LOG.warn("Cound not close the gzip output stream", e);
                }
                out = null;
            }
            if (client != null && !client.isClosed()) {
                try {
                    client.close();
                } catch (IOException e) {
                    LOG.warn("Cound not close the socket from " + client.getInetAddress(), e);
                }
                client = null;
            }
            if (!recoverySuccess) {
                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {}
            }
            mgr.reportRecovery(ident, recoverySuccess);
        }
    }
}

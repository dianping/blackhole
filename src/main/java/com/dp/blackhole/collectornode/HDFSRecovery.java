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

import com.dp.blackhole.common.BlackholeException;
import com.dp.blackhole.common.Util;

public class HDFSRecovery implements Runnable{
    private static final Log LOG = LogFactory.getLog(HDFSUpload.class);
    private Collectornode node;
    private FileSystem fs;
    private static final int DEFAULT_BUFSIZE = 8192;
    private Socket client;
    private String appName;
    private String appHost;
    private String fileSuffix;
    private long position;
    private long length;
    private boolean recoverySuccess;
    
    public HDFSRecovery(Collectornode node, FileSystem fs, Socket client, 
            String appName, String appHost, String fileSuffix) {
        this.node = node;
        this.fs = fs;
        this.client = client;
        this.appName = appName;
        this.appHost = appHost;
        this.fileSuffix = fileSuffix;
    }
    
    public HDFSRecovery(Collectornode node, FileSystem fs, Socket client, 
            String appName, String appHost, String fileSuffix,
            long position, long length) {
        this.node = node;
        this.fs = fs;
        this.client = client;
        this.appName = appName;
        this.appHost = appHost;
        this.fileSuffix = fileSuffix;
        this.position = position;
        this.length = length;
    }
    
    @Override
    public void run() {
        GZIPOutputStream gout = null;
        GZIPInputStream gin = null;
        try {
            String oldPathStr = Util.getHDFSPathByIdent(appName, appHost, fileSuffix);
            Path old = new Path(oldPathStr);
            long offset = 0;
            if (fs.exists(old) && fs.isFile(old)) {
                Path dst = new Path(old.getParent(), old.getName() + ".r");
                gout = new GZIPOutputStream(fs.create(dst));//TODO need buffer?
                
                gin = new GZIPInputStream(fs.open(old));
                int len = 0;
                byte[] buf = new byte[DEFAULT_BUFSIZE];
                while((len = gin.read(buf)) != -1) {
                    gout.write(buf, 0, len);
                    offset += len;
                }
                LOG.info("Reloaded the hdfs file " + old);

                DataOutputStream out = new DataOutputStream(client.getOutputStream());
                out.writeLong(offset);
                LOG.info("Send an offset [" + offset + "] to client");
                
                BufferedInputStream in = new BufferedInputStream(client.getInputStream());
                while((len = in.read(buf)) != -1) {
                    gout.write(buf, 0, len);
                }
                LOG.info("Finished to write " + dst);
                
                if (fs.delete(old, false)) {
                    if (fs.rename(dst, old)) {
                        LOG.info("Finished to rename to " + old);
                    } else {
                        throw new BlackholeException("Faild to rename to " + old);
                    }
                } else {
                    throw new BlackholeException("Faild to delete " + old + " before mv.");
                }
            } else {
                throw new BlackholeException("Can not found the file in HDFS");
            }
            recoverySuccess = true;
        } catch (IOException e) {
            recoverySuccess = false;
            LOG.error("Oops, got an exception:", e);
        } catch (Exception e) {
            recoverySuccess = false;
            LOG.error("Oops, got an exception:", e);
        } finally {
            node.recoveryResult(this, recoverySuccess);
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
            //TODO should I close the fs?
        }
    }
}

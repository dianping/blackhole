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

import com.dp.blackhole.common.Util;

public class HDFSRecovery implements Runnable{
    private static final Log LOG = LogFactory.getLog(HDFSUpload.class);
    private static final String TMP_SUFFIX = ".tmp";
    private static final String R_SUFFIX = ".r";

    private Collectornode node;
    private FileSystem fs;
    private String baseHDFSPath;
    private static final int DEFAULT_BUFSIZE = 8192;
    private Socket client;
    private String appName;
    private String appHost;
    private String fileSuffix;
    private boolean recoverySuccess;
    
    public HDFSRecovery(Collectornode node, FileSystem fs, String baseHDFSPath, 
            Socket client, String appName, String appHost, String fileSuffix) {
        this.node = node;
        this.fs = fs;
        this.baseHDFSPath = baseHDFSPath;
        this.client = client;
        this.appName = appName;
        this.appHost = appHost;
        this.fileSuffix = fileSuffix;
        this.recoverySuccess = false;
    }

    @Override
    public void run() {
        GZIPOutputStream gout = null;
        GZIPInputStream gin = null;
        try {
            String normalPathname = Util.getHDFSPathByIdent(baseHDFSPath, appName, appHost, fileSuffix);
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
                }
                DataOutputStream out = new DataOutputStream(client.getOutputStream());
                out.writeLong(offset);
                LOG.info("Send an offset [" + offset + "] to client");
                
                BufferedInputStream in = new BufferedInputStream(client.getInputStream());
                while((len = in.read(buf)) != -1) {
                    gout.write(buf, 0, len);
                }
                LOG.info("Finished to write " + recoveryPath);
                
                if (Util.multipleRename(fs, recoveryPath, normalPath)) {
                    LOG.info("Finished to rename to " + normalPathname);
                } else {
                    throw new Exception("Faild to rename to " + normalPathname);
                }
                
                if (fs.exists(tmpPath) && !Util.multipleDelete(fs, tmpPath)) {   //delete .tmp if exists
                    throw new Exception("Faild to delete recovery file " + tmpPathname);
                }
            } else {    //When normal path exists then just delete .tmp and .r file if they exist.
                if (fs.exists(tmpPath) && !Util.multipleDelete(fs, tmpPath)) {
                    throw new Exception("Faild to delete recovery file " + tmpPathname);
                }
            }
            recoverySuccess = true;
        } catch (Exception e) {
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
        }
    }
}

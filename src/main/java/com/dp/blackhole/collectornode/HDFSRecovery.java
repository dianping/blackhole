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
    private static final Log LOG = LogFactory.getLog(HDFSWriter.class);
    private FileSystem fs;
    private static final int DEFAULT_BUFSIZE = 8102;
    private Socket client;
    private int position;
    private long length;
    private String appName;
    private String ident;
    
    public HDFSRecovery(FileSystem fs, Socket client, String appName, String ident) {
        this.fs = fs;
        this.client = client;
        this.appName = appName;
        this.ident = ident;
    }
    
    public HDFSRecovery(FileSystem fs, Socket client, String appName, String ident,
            int position, long length) {
        this.fs = fs;
        this.client = client;
        this.appName = appName;
        this.ident = ident;
        this.position = position;
        this.length = length;
    }
    
    @Override
    public void run() {
        GZIPOutputStream gout = null;
        try {
            String oldPathStr = Util.getHDFSPathByIdent(appName, ident);
            Path old = new Path(oldPathStr);
            long offset = 0;
            if (fs.exists(old) && fs.isFile(old)) {
                Path dst = new Path("." + oldPathStr);
                gout = new GZIPOutputStream(fs.create(dst));
                GZIPInputStream gin = new GZIPInputStream(fs.open(old));
                int len = 0;
                byte[] buf = new byte[DEFAULT_BUFSIZE]; 
                while((len = gin.read(buf)) != -1) {
                    gout.write(buf);
                    offset += len;
                }
                gin.close();
                LOG.info("Reload the data of hdfs file " + old + " completed.");
                
                DataOutputStream out = new DataOutputStream(client.getOutputStream());
                out.writeLong(offset);
                out.close();
                LOG.info("Send an offset[" + offset + "] to client to load the uncompleted data");
                
                BufferedInputStream in = new BufferedInputStream(client.getInputStream());
                while((len = in.read(buf)) != -1) {
                    gout.write(buf);
                }
                LOG.info("Write to a new hdfs file " + dst + " completed.");
                
                if (fs.delete(old, false)) {
                    if (fs.rename(dst, old)) {
                        LOG.info("Rename " + dst + " to " + old);
                    } else {
                        LOG.error("Faild to rename " + dst + " to " + old);
                    }
                } else {
                    LOG.warn("Faild to delete " + old + " before mv.");
                }
                
            } else {
                LOG.error("Can not found the file in HDFS");
            }
        } catch (IOException e) {
            LOG.error("Oops, got an exception:", e);
        } finally {
            try {
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

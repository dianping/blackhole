package com.dp.blackhole.collectornode;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dp.blackhole.common.BlackholeException;

public class HDFSUpload implements Runnable{
    private static final Log LOG = LogFactory.getLog(HDFSUpload.class);
    private Collectornode node;
    private FileSystem fs;
    private static final String RAF_MODE = "r";
    private static final int DEFAULT_BUFSIZE = 8192;
    private boolean delsrc;
    private int position;
    private long length;
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
        this.position = -1;
    }
    public HDFSUpload(Collectornode node, FileSystem fs, File file, String dfsPath, boolean delsrc, 
            int position, long length) {
        this.node = node;
        this.fs = fs;
        this.file = file;
        this.dfsPath = dfsPath;
        this.delsrc = delsrc;
        this.position = position;
        this.length = length;
    }
    @Override
    public void run() {
        if (!file.isFile()) {
            LOG.warn(file + "is not a FILE. Quite.");
            return;
        }
        Path src = new Path(file.getPath());
        Path tmp = new Path(dfsPath + ".tmp");
        RandomAccessFile reader = null;
        
        try {
            if (position == -1) {
                fs.copyFromLocalFile(delsrc, true, src, tmp);
                LOG.info("Collector file " + file + " has been uploaded. " +
                		"Has deleted it? "+ delsrc);
            } else {
                long count = 0;
                byte[] inbuf = new byte[DEFAULT_BUFSIZE];
                reader = new RandomAccessFile(file, RAF_MODE);
                out = fs.create(tmp);
                reader.seek(position);
                LOG.info("Seek to the position " + position + " ok. Begin to upload...");
                int len = 0;
                while (count != length && (len = reader.read(inbuf)) != -1) {
                    out.write(inbuf, 0, len);
                    out.flush();
                    count += len;
                }
                LOG.info("Collector file " + file + " has been uploaded " + count + " bytes.");
                if (delsrc) {
                    LOG.info("Delete src file " + file);
                    if (!file.delete())
                        LOG.warn("Delete src file " + file + " faild.");
                }
            }
            //rename
            Path dst = new Path(dfsPath);
            if (!fs.rename(tmp, dst)) {
                throw new BlackholeException("Faild to rename tmp to " + dst);
            }
            uploadSuccess = true;
        } catch (IOException e) {
            uploadSuccess = false;
            LOG.error("Oops, got an exception:", e);
        } catch (Exception e) {
            uploadSuccess = false;
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

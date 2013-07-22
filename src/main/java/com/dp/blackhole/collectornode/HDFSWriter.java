package com.dp.blackhole.collectornode;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSWriter implements Runnable{
  private static final Log LOG = LogFactory.getLog(HDFSWriter.class);
  private FileSystem fs;
  private static final String RAF_MODE = "r";
  private static final int DEFAULT_BUFSIZE = 8102;
  private boolean delsrc;
  private int position;
  private long length;
  private File file;
  private String dfsPath;
  private DataOutputStream out;
  
  public HDFSWriter(FileSystem fs, File file, String dfsPath, boolean delsrc) {
    this.fs = fs;
    this.file = file;
    this.dfsPath = dfsPath;
    this.delsrc = delsrc;
    this.position = -1;
  }
  public HDFSWriter(FileSystem fs, File file, String dfsPath, boolean delsrc, 
      int position, long length) {
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
    Path dst = new Path(dfsPath);
    RandomAccessFile reader = null;
    
    try {
      if (position == -1) {
        fs.copyFromLocalFile(delsrc, true, src, dst);
        LOG.info("Collector file " + file + " has been uploaded and deleted.");
      } else {
        long count = 0;
        byte[] inbuf = new byte[DEFAULT_BUFSIZE];
        reader = new RandomAccessFile(file, RAF_MODE);
        out = fs.create(dst);
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
    } catch (IOException e) {
      LOG.error("Oops, got an exception:", e);
    } finally {
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

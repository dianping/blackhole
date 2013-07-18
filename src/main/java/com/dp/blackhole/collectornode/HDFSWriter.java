package com.dp.blackhole.collectornode;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.SocketOutputStream;

public class HDFSWriter implements Runnable{
  private static final Log LOG = LogFactory.getLog(HDFSWriter.class);
  private FileSystem fs;
  private int position;
  private int count;
  private File file;
  private String dfsPath;
  private FSDataInputStream fin;
  private FSDataOutputStream fout;
  private FileChannel fileChannel;
  
  private DataOutputStream out;
  
  public HDFSWriter(FileSystem fs, File file, String dfsPath, int position, int count) {
    this.fs = fs;
    this.file = file;
    this.dfsPath = dfsPath;
    this.position = position;
    this.count = count;
  }
  @Override
  public void run() {
    try {
      fileChannel = new FileInputStream(file).getChannel();
      fout = fs.create(new Path(dfsPath));
      OutputStream stream = fout.getWrappedStream();
//      fileChannel.transferTo(position, count, target)
      if (stream instanceof SocketOutputStream) {
        System.out.println("return a socket out put stream. transfer to fully.");
        ((SocketOutputStream) stream).transferToFully(fileChannel, position, count);
      } else {
        System.out.println("Oh, no!");
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        fout.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

}

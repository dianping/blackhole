package com.dp.blackhole.appnode;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.util.AppUtil;

public class LogLoader implements Runnable{
  private static final Log LOG = LogFactory.getLog(LogLoader.class);
  private static final String RAF_MODE = "r";
  private static final int DEFAULT_BUFSIZE = 8102;
  private AppLog appLog;
//  private Appnode appnode;  //to persist it for next version
  private String rollIdent;
  private long offset;
  private Socket server;
  private byte[] inbuf;
  public LogLoader(AppLog appLog, String rollIdent, long offset) {
//    this.appnode = appnode;
    this.appLog = appLog;
    this.rollIdent = rollIdent;
    this.offset = offset;
    this.inbuf = new byte[DEFAULT_BUFSIZE];
  }
  
  @Override
  public void run() {
    File rolledFile = AppUtil.findRealFileByIdent(appLog, rollIdent);
    RandomAccessFile reader = null;
    DataOutputStream out = null;
    try {
      server = new Socket(appLog.getServer(), appLog.getPort());
      out = new DataOutputStream(server.getOutputStream());
      reader = new RandomAccessFile(rolledFile, RAF_MODE);
//      long length = rolledFile.length();
      reader.seek(offset);
      LOG.info("Seek to the position " + offset + " ok. Begin to transfer...");
      int len = 0;
      while ((len = reader.read(inbuf)) != -1) {
        out.write(inbuf, 0, len);
        out.flush();
      }
      LOG.info("Roll file " + rolledFile + " has been transfered");
    } catch (FileNotFoundException e) {
      LOG.error(e);
    } catch (UnknownHostException e) {
      LOG.error("Faild to build a socket with host:" 
          + appLog.getServer() + " port:" + appLog.getPort() + e);
    } catch (IOException e) {
      LOG.error("Faild to build Input/Output stream. " + e);
    } finally {
      try {
        if (out != null) {
          out.close();
        }
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }


}

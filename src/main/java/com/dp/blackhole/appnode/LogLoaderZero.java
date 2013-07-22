package com.dp.blackhole.appnode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.util.AppUtil;

public class LogLoaderZero implements Runnable{
  private static final Log LOG = LogFactory.getLog(LogLoaderZero.class);
  private AppLog appLog;
  private String rollIdent;
  private long offset;
  public LogLoaderZero(AppLog appLog, String rollIdent, long offset) {
    this.appLog = appLog;
    this.rollIdent = rollIdent;
    this.offset = offset;
  }
  
  @Override
  public void run() {
    File rolledFile = AppUtil.findRealFileByIdent(appLog, rollIdent);
    if (rolledFile == null) {
      LOG.error("Can not find the file match rollIdent " + rollIdent);
      return;
    }
    SocketChannel socketChannel = null;
    try {
      SocketAddress address = new InetSocketAddress(appLog.getServer(), appLog.getPort());
      socketChannel = SocketChannel.open();
      socketChannel.connect(address);
      socketChannel.configureBlocking(true);
      FileChannel fc = new FileInputStream(rolledFile).getChannel();
      long curnset =  fc.transferTo(offset, rolledFile.length(), socketChannel);//TODO to be review
      LOG.info("Roll file " + rolledFile + " has been transfered, ");
    } catch (IOException e) {
      LOG.error("Oops, got an exception:", e);
    } finally {
      if (socketChannel != null) {
        try {
          socketChannel.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }


}

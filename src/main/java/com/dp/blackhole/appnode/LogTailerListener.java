package com.dp.blackhole.appnode;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.util.AppUtil;
import com.dp.blackhole.util.TestMode;

public class LogTailerListener implements TailerListener {
  public static final Log LOG = LogFactory.getLog(LogTailerListener.class);

  private AppLog appLog;
  private Appnode appnode;
  private Socket server;
  private Tailer tailer;
  private OutputStreamWriter writer;
  public LogTailerListener(Appnode appnode, AppLog appLog) {
    this.appnode = appnode;
    this.appLog = appLog;
  }

  /**
   * The tailer will call this method during construction,
   * giving the listener a method of stopping the tailer.
   * @param tailer the tailer.
   */
  public void init(Tailer tailer) {
    this.tailer = tailer;
    LOG.debug("listening the applog: " + appLog.getAppName());
    try {
      server = new Socket(appLog.getServer(), appLog.getPort());
      writer = new OutputStreamWriter(server.getOutputStream());
    } catch (UnknownHostException e) {
      tailer.stop();
      LOG.error("Faild to build a socket with host:" 
          + appLog.getServer() + " port:" + appLog.getPort()
      		+ "This tailer thread stop! ", e);
    } catch (IOException e) {
      tailer.stop();
      LOG.error("Faild to build a socket. " +
      		"This tailer thread stop! ", e);
    }
  }

  /**
   * This method is called if the tailed file is not found.
   */
  public void fileNotFound(){
    LOG.warn("File " + appLog.getTailFile() + " not found");
  }

  /**
   * Called if a file rotation is detected.
   * Insert a "" string to tail stream to distinguish 
   * different interval file like "trace.log.2013-07-11.12".
   * And, send a message APP_ROLL to supervisor 
   * which include file identify and its length.
   * 
   * This method is called before the file is reopened, and fileNotFound may
   * be called if the new file has not yet been created.
   */
  public void fileRotated() {
    handle("");
    int interval = 60;//TODO next version, read from configure.
    String rollIdent = AppUtil.getRollIdentByTime(new Date(), interval);
    File rollFile = AppUtil.findRealFileByIdent(appLog, rollIdent);
    if (rollFile != null) {
      long fileLength = rollFile.length();
      if (!TestMode.test) {
        appnode.roll(appLog.getAppName(), rollIdent, fileLength);
      }
      LOG.info("File rotation is deteced. Roll file is " + rollFile
      		+ ", size is " + fileLength);
    }
  }

  /**
   * Handles a line from a Tailer.
   * @param line the line.
   */
  public void handle(String line) {
    try {
//    if(isServerLive()) {
      writer.write(line);
      writer.write('\n'); //make server easy to handle
      writer.flush();
      LOG.debug("client>" + line);
//    } else {
//      //maybe stop the thread
//    }
    } catch (IOException e) {
      LOG.error("Oops, got an exception:", e);
    }
  }

  /**
   * Handles an Exception .
   * @param ex the exception.
   */
  public void handle(Exception ex) {
    LOG.error("Oops, got an exception:", ex);
  }
  
//  public boolean isServerLive() {
//    try{
//      server.sendUrgentData(0xFF);
//      return true;
//    }catch(Exception ex){
//      return false;
//    }
//  }
}
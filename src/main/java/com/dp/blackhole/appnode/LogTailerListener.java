package com.dp.blackhole.appnode;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.util.DateUtil;

public class LogTailerListener implements TailerListener {
  public static final Log LOG = LogFactory.getLog(LogTailerListener.class);
  private AppLog appLog;
  private Appnode appnode;
  private Socket server;
  private static long fileLength;
//  private Tailer tailer;
  public LogTailerListener(Appnode appnode, AppLog appLog) {
    this.appnode = appnode;
    this.appLog = appLog;
    fileLength = 0;
  }
  
  /**
   * The tailer will call this method during construction,
   * giving the listener a method of stopping the tailer.
   * @param tailer the tailer.
   */
  public void init(Tailer tailer) {
//    this.tailer = tailer;
    LOG.debug("listening the applog: " + appLog.getAppName());
    try {
      server = new Socket(appLog.getServer(), appLog.getPort());
    } catch (UnknownHostException e) {
      tailer.stop();
      LOG.error("Faild to build a socket with host:" 
          + appLog.getServer() + " port:" + appLog.getPort()
      		+ "This tailer thread stop! " + e);
    } catch (IOException e) {
      tailer.stop();
      LOG.error("Faild to build a socket. " +
      		"This tailer thread stop! " + e);
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
   *
   * This method is called before the file is reopened, and fileNotFound may
   * be called if the new file has not yet been created.
   */
  public void fileRotated() {//TODO SIZE
    int interval = 60;//TODO next version, read from configure trace.log.2013-07-11.12
    String rollIdent = DateUtil.getRollIdent(new Date(), interval);
    appnode.roll(appLog.getAppName(), rollIdent, fileLength);
    LOG.info("File rotation is deteced. Roll file identify is ," + rollIdent
    		+ ", length is " + fileLength);
    fileLength = 0;
  }

  /**
   * Handles a line from a Tailer.
   * @param line the line.
   */
  public void handle(String line) {
    OutputStreamWriter writer = null;
    try {
      writer = new OutputStreamWriter(server.getOutputStream());
//      if(isServerLive()) {
        writer.write(line);
        writer.flush();
        LOG.debug("client>" + line);
//      } else {
//        //stop the thread
//      }
      fileLength++;
    } catch (IOException e) {
      LOG.error(e);
    }
  }

  /**
   * Handles an Exception .
   * @param ex the exception.
   */
  public void handle(Exception ex) {
    LOG.error(ex);
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
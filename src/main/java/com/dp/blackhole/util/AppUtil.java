package com.dp.blackhole.util;

import java.io.File;
import java.io.FileFilter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.appnode.AppLog;

public class AppUtil {
  
  private static final Log LOG = LogFactory.getLog(AppUtil.class);
  
  public static String getRollIdentByTime(Date date, int interval) {
    //first version, we use 60 min (1 hour) as fixed interval
    SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd.HH:00:00");
    return dayFormat.format(getOneHoursAgoTime(date));
  }
  
  public static Date getOneHoursAgoTime(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.HOUR, -1);
    return cal.getTime();
  }
  
  public static File findRealFileByIdent(AppLog appLog, final String rollIdent) {
    // real file: trace.log.2013-07-11.12
    // rollIdent: 2013-07-11.12:00:00
    FileFilter filter = new FileFilter() {
      public boolean accept(File pathName) {
        CharSequence rollIdentSequence = rollIdent.subSequence(0, rollIdent.indexOf(':'));
        LOG.debug("rollIdent sequence is " + rollIdentSequence);
        if ((pathName.getName().contains(rollIdentSequence))) {
          return true;
        }
        return false;
      }
    };
    int index = appLog.getTailFile().lastIndexOf('/');
    String directoryStr = appLog.getTailFile().substring(0, index);
    LOG.debug("DIR IS " + directoryStr);
    List<File> candidateFiles = Arrays.asList(new File(directoryStr).listFiles(filter));
   
    if (candidateFiles.isEmpty()) {
      LOG.error("Can not find any candidate file for rollIdent " + rollIdent);
      return null;
    } else if (candidateFiles.size() > 1) {
      LOG.error("CandidateFile number is more then one. It isn't an expected result." +
          "CandidateFiles are " +  Arrays.toString(candidateFiles.toArray()));
      return null;
    } else {
      return candidateFiles.get(0);
    }
  }
}

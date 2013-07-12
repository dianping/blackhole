package com.dp.blackhole.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

  public static String getRollIdent(Date date, int interval) {
    //first version, we use 60 min (1 hour) as fixed interval
    SimpleDateFormat formatForHour = new SimpleDateFormat("yyyy-MM-dd.HH:00:00");
    return formatForHour.format(date);
  }
}

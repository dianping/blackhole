package com.dp.blackhole.check;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class Util {
    private final static Log LOG = LogFactory.getLog(Util.class);
    private static long localTimezoneOffset = TimeZone.getTimeZone("Asia/Shanghai").getRawOffset();
    private static final int REPEATE = 3;
    private static final int RETRY_SLEEP_TIME = 1000;
    public static final String DONE_FLAG = "_done";
    public static final String TIMEOUT_FLAG = "_timeout";
    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String getDatepathbyFormat (String format) {
        StringBuffer dirs = new StringBuffer();
        for (String dir: format.split("\\.")) {
            dirs.append(dir);
            dirs.append('/');
        }
        return dirs.toString();
    }

    public static String getFormatFromPeroid (long period) {
        String format;
        if (period < 60) {
            format = "yyyy-MM-dd.HH.mm.ss";
        } else if (period < 3600) {
            format = "yyyy-MM-dd.HH.mm";
        } else if (period < 86400) {
            format = "yyyy-MM-dd.HH";
        } else {
            format = "yyyy-MM-dd";
        }
        return format;
    }

    public static long getPrevWholeTs(long ts, long rollPeriod) {
        return getWholeTs(ts, rollPeriod, -1);
    }

    public static long getCurrWholeTs(long ts, long rollPeriod) {
        return getWholeTs(ts, rollPeriod, 0);
    }

    public static long getNextWholeTs(long ts, long rollPeriod) {
        return getWholeTs(ts, rollPeriod, 1);
    }

    private static long getWholeTs(long ts, long rollPeriod, int offset) {
        rollPeriod = rollPeriod * 1000;
        ts = ts + localTimezoneOffset;
        long ret = (ts / rollPeriod + offset) * rollPeriod;
        ret = ret - localTimezoneOffset;
        return ret;
    }

    /*
     * Path format:
     * hdfsbasedir/appname/2013-11-01/14/08/machine01@appname_2013-11-01.14.08.gz.tmp
     * hdfsbasedir/appname/2013-11-01/14/08/machine02@appname_2013-11-01.14.08.gz.tmp
     */
    public static Path[] getRollHdfsPath (RollIdent ident, String source) {
        return getRollHdfsPathByTs(ident, ident.ts, source, false);
    }

    public static Path[] getRollHdfsPathByTs (RollIdent ident, long checkTs, String source, boolean hidden) {
        String format  = Util.getFormatFromPeroid(ident.period);
        Date roll = new Date(checkTs);
        SimpleDateFormat dm= new SimpleDateFormat(format);
        if (hidden) {
            Path[] hiddenPath = new Path[1];
            hiddenPath[0] = new Path(CheckDone.hdfsbasedir + '/' + ident.app + '/' + getDatepathbyFormat(dm.format(roll)) +
                    CheckDone.hdfsHiddenfileprefix + source + '@' + ident.app + "_" + dm.format(roll));
            return hiddenPath;
        } else {
            Path[] rollPath = new Path[CheckDone.hdfsfilesuffix.length];
            for(int i =0 ;i < CheckDone.hdfsfilesuffix.length; i++) {
                rollPath[i] = new Path(CheckDone.hdfsbasedir + '/' + ident.app + '/' + getDatepathbyFormat(dm.format(roll)) +
                        source + '@' + ident.app + "_" + dm.format(roll) + "." + CheckDone.hdfsfilesuffix[i]);
            }
            return rollPath;
        }
    }
    
    public static boolean retryExists(Path[] expecteds) {
        for (int i = 0; i < expecteds.length; i++) {
            Path path = expecteds[i];
            if (retryExists(path)) {
                return true;
            }
        }
        return false;
    }

    public static boolean retryExists(Path expected) {
        for (int i = 0; i < REPEATE; i++) {
            try {
                return CheckDone.fs.exists(expected);
            } catch (IOException e) {
            }
            try {
                Thread.sleep(RETRY_SLEEP_TIME);
            } catch (InterruptedException ex) {
                return false;
            }
        }
        return false;
    }

    public static boolean retryTouch(Path parentPath, String flag) {
        FSDataOutputStream out = null;
        Path doneFile = new Path(parentPath, flag);
        for (int i = 0; i < REPEATE; i++) {
            try {
                out = CheckDone.fs.create(doneFile);
                return true;
            } catch (IOException e) {
            } finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException e) {
                        LOG.warn("Close hdfs out put stream fail!", e);
                    }
                }
            }
            try {
                Thread.sleep(RETRY_SLEEP_TIME);
            } catch (InterruptedException ex) {
                return false;
            }
        }
        return false;
    }

    public static boolean wasDone (RollIdent ident, long ts) {
        String format  = Util.getFormatFromPeroid(ident.period);
        Date roll = new Date(ts);
        SimpleDateFormat dm= new SimpleDateFormat(format);
        Path done =  new Path(CheckDone.hdfsbasedir + '/' + ident.app + '/' +
                Util.getDatepathbyFormat(dm.format(roll)) + DONE_FLAG);
        if (Util.retryExists(done)) {
            return true;
        } else {
            Path succ =  new Path(CheckDone.hdfsbasedir + '/' + ident.app + '/' +
                    Util.getDatepathbyFormat(dm.format(roll)) + CheckDone.successprefix + dm.format(roll));
            return Util.retryExists(succ);
        }
    }
    
    public static String[] getStringListOfLionValue(String rawValue) {
        if (rawValue == null) {
            return null;
        }
        String value = rawValue.trim();
        if (value.charAt(0) != '[' || value.charAt(value.length() - 1) != ']') {
            return null;
        }
        String[] tmp = value.substring(1, value.length() - 1).split(",");
        String[] result = new String[tmp.length];
        for (int i = 0; i < tmp.length; i++) {
            result[i] = tmp[i].trim().substring(1, tmp[i].trim().length() -1 );
        }
        return result;
    }

    //["host01","host02"]
    public static String getLionValueOfStringList(String[] hosts) {
        StringBuilder lionStringBuilder = new StringBuilder();
        lionStringBuilder.append('[');
        for (int i = 0; i < hosts.length; i++) {
            lionStringBuilder.append('"').append(hosts[i]).append('"');
            if (i != hosts.length - 1) {
                lionStringBuilder.append(',');
            }
        }
        lionStringBuilder.append(']');
        return lionStringBuilder.toString();
    }

    public static String[][] getStringMapOfLionValue(String rawValue) {
        if (rawValue == null) {
            return null;
        }
        String value = rawValue.trim();
        if (value.charAt(0) != '{' || value.charAt(value.length() - 1) != '}') {
            return null;
        }
        String[] tmp = value.substring(1, value.length() - 1).split(",");
        if (tmp.length == 0) {
            return null;
        }
        String[][] result = new String[tmp.length][2];
        for (int i = 0; i < tmp.length; i++) {
            String[] tmp2 = tmp[i].trim().split(":");
            for (int j = 0; j < 2; j++) {
                result[i][j] = tmp2[j].trim().substring(1, tmp2[j].trim().length() -1);
            }
        }
        return result;
    }
}

package com.dp.blackhole.check;

public class Util {
    private static long magic = 8 * 3600 * 1000l;
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
        rollPeriod = rollPeriod * 1000;
        long ret = (ts / rollPeriod - 1) * rollPeriod;
        if (rollPeriod >= magic) {
            ret = ret - magic;
        }
        return ret;
    }
    
    public static long getCurrWholeTs(long ts, long rollPeriod) {
        rollPeriod = rollPeriod * 1000;
        long ret = (ts / rollPeriod) * rollPeriod;
        if (rollPeriod >= magic) {
            ret = ret - magic;
        }
        return ret;
    }
    
    public static long getNextWholeTs(long ts, long rollPeriod) {
        rollPeriod = rollPeriod * 1000;
        long ret = (ts / rollPeriod + 1) * rollPeriod;
        if (rollPeriod >= magic) {
            ret = ret - magic;
        }
        return ret;
    }
}

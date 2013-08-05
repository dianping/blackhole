package com.dp.blackhole.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Util {
    private static final Log LOG = LogFactory.getLog(Util.class);
    private static final int REPEATE = 3;
    private static final int RETRY_SLEEP_TIME = 3000;

    public static boolean retryDelete(FileSystem fs, Path path) {
        for (int i = 0; i < REPEATE; i++) {
            try {
                if (fs.delete(path, false)) {
                    return true;
                }
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
    
    public static boolean retryRename(FileSystem fs, Path src, Path dst) {
        for (int i = 0; i < REPEATE; i++) {
            try {
                if (fs.rename(src, dst)) {
                    return true;
                }
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

    public static Date getOneHoursAgoTime(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.HOUR, -1);
        return cal.getTime();
    }

    public static File findRealFileByIdent(String appTailFile, final String rollIdent) {
        // real file: trace.log.2013-07-11.12
        // rollIdent is "2013-07-11.12" as long as time unit is "hour"
        String directoryStr = appTailFile.substring(0, appTailFile.lastIndexOf('/'));
        LOG.debug("DIR IS " + directoryStr);
        
        FileFilter filter = new FileFilter() {
            public boolean accept(File pathName) {
                CharSequence rollIdentSequence = rollIdent;
                LOG.debug("rollIdent sequence is " + rollIdentSequence);
                if ((pathName.getName().contains(rollIdentSequence))) {
                    return true;
                }
                return false;
            }
        };
        List<File> candidateFiles = Arrays.asList(new File(directoryStr).listFiles(filter));
        if (candidateFiles.isEmpty()) {
            LOG.error("Can not find any candidate file for rollIdent " + rollIdent);
            return null;
        } else if (candidateFiles.size() > 1) {
            LOG.error("CandidateFile number is more then one. It isn't an expected result." +
                    "CandidateFiles are " +    Arrays.toString(candidateFiles.toArray()));
            return null;
        } else {
            return candidateFiles.get(0);
        }
    }

    @Deprecated
    public static long getPeriodInSeconds(int value, String unit) {
        if (unit.equalsIgnoreCase("hour")) {
            return 3600 * value;
        } else if (unit.equalsIgnoreCase("day")) {
            return 3600 * 24 * value;
        } else if (unit.equalsIgnoreCase("minute")) {
            return 60 * value;
        } else {
            LOG.warn("Period unit is not valid, use hour for default.");
            return 3600 * value;
        }
    }

    public static String getFormatFromPeroid (long period) {
        String format;
        if (period < 60) {
            format = "yyyy-MM-dd.hh:mm:ss";
        } else if (period < 3600) {
            format = "yyyy-MM-dd.hh:mm";
        } else if (period < 86400) {
            format = "yyyy-MM-dd.hh";
        } else {
            format = "yyyy-MM-dd";
        }
        return format;
    }

    public static void writeString(String str, SocketChannel channel) throws IOException {
        byte[] data = str.getBytes();
        ByteBuffer writeBuffer = ByteBuffer.allocate(4 + data.length);
        writeBuffer.putInt(data.length);
        writeBuffer.put(data);
        writeBuffer.flip();
        while (writeBuffer.remaining() != 0) {
            channel.write(writeBuffer);
        }
    }

    public static void writeLong(long period, SocketChannel channel) throws IOException {
        ByteBuffer writeBuffer = ByteBuffer.allocate(8);
        writeBuffer.putLong(period);
        writeBuffer.flip();
        while (writeBuffer.remaining() != 0) {
            channel.write(writeBuffer);
        }
    }

    public static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        return new String(data);
    }

    public static void writeString(String str ,DataOutputStream out) throws IOException {
        byte[] data = str.getBytes();
        out.writeInt(data.length);
        out.write(data);
    }
    
    public static long getTS() {
        Date now = new Date();
        return now.getTime();
    }
    
    public static long getRollTs(long rollPeriod) {
        long ts = Util.getTS();
        
        if ((ts % rollPeriod) < (rollPeriod/2)) {
            ts = (ts / rollPeriod) * rollPeriod * 1000;
        } else {
            ts = (ts / rollPeriod + 1) * rollPeriod *1000;
        }
        return ts;
    }
}

package com.dp.blackhole.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Util {
    private static final Log LOG = LogFactory.getLog(Util.class);
    private static final int REPEATE = 3;
    private static final int RETRY_SLEEP_TIME = 3000;
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static long magic = 8 * 3600 * 1000l;
    
    public static String getRemoteHost(Socket socket) {
      InetSocketAddress remoteAddr= ((InetSocketAddress)socket.getRemoteSocketAddress());
      return remoteAddr.getHostName();
    }
    
    public static String getLocalHost() throws UnknownHostException {
      return InetAddress.getLocalHost().getHostName();
    }
    
    public static String ts2String(long ts) {
        return (new Date(ts)).toString();
    }
    
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
        File realFile = new File(appTailFile + "." + rollIdent);
        if (realFile.isFile() && realFile.exists()) {
            return realFile;
        } else {
            return null;
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
    
    /*
     * get roll timestamp, for example
     * now is 16:02, and rollPeroid is 1 hour, then
     * return st of 16:00
     */
    public static long getRollTs(long ts, long rollPeriod) {
        rollPeriod = rollPeriod * 1000;
        long ret = (ts / rollPeriod) * rollPeriod;
        if (rollPeriod >= magic) {
            ret = ret - magic;
        }
        return ret;
    }
    
    /*
     * get the closest roll timestamp, for example
     * now is 16:02, and rollPeroid is 1 hour, then
     * return ts of 15:00;
     * now is 15:59, and rollPeroid is 1 hour, then
     * return ts of 15:00
     */
    public static long getClosestRollTs(long ts, long rollPeriod) {
        rollPeriod = rollPeriod * 1000;
        
        if ((ts % rollPeriod) < (rollPeriod/2)) {
            ts = (ts / rollPeriod -1) * rollPeriod;
        } else {
            ts = (ts / rollPeriod) * rollPeriod;
        }
        
        //TODO 1378443602000 will get wrong result
        if (rollPeriod >= magic) {
            ts = ts - magic;
        }

        return ts;
    }
    
    public static String getParentAbsolutePath(String absolutePath) {
        return absolutePath.substring(0, absolutePath.lastIndexOf(
                System.getProperty("file.separator")));
    }
    
    public static String formatTs(long ts) {
        return format.format(new Date(ts));
    }
}

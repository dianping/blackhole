package com.dp.blackhole.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Util {
    private static final Log LOG = LogFactory.getLog(Util.class);
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static long localTimezoneOffset = TimeZone.getTimeZone("Asia/Shanghai").getRawOffset();
    
    public static String getRemoteHost(Socket socket) {
      InetSocketAddress remoteAddr= ((InetSocketAddress)socket.getRemoteSocketAddress());
      return remoteAddr.getHostName();
    }
    
    public static String getRemoteHostAndPort(Socket socket) {
        InetSocketAddress remoteAddr= ((InetSocketAddress)socket.getRemoteSocketAddress());
        return remoteAddr.toString();
      }
    
    public static String getLocalHost() throws UnknownHostException {
      return InetAddress.getLocalHost().getHostName();
    }
    
    public static String ts2String(long ts) {
        return (new Date(ts)).toString();
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
        return new File(appTailFile + "." + rollIdent);
    }

    public static File findGZFileByIdent(final String appTailFile, final String rollIdent) {
        try {
            final int indexOfLastSlash = appTailFile.lastIndexOf('/');
            File root = new File(appTailFile.substring(0, indexOfLastSlash + 1)); 
            File[] files = root.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    String gzFileRegex = "[_a-z0-9-\\.]+"
                            + "__"
                            + "[_a-z0-9-\\.]+"
                            + appTailFile.substring(indexOfLastSlash + 1)
                            + "\\."
                            + rollIdent
                            + "\\.gz";
                    Pattern p = Pattern.compile(gzFileRegex);
                    return p.matcher(name).matches();
                }
            });
            if (files == null || files.length == 0) {
                return null;
            } else {
                return files[0];
            }
        } catch (StringIndexOutOfBoundsException e) {
            LOG.error("Handle " + appTailFile + " " + rollIdent, e);
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

    public static void writeString(String str, ByteBuffer buffer) {
        byte[] data = str.getBytes();
        buffer.putInt(data.length);
        buffer.put(data);
    }
    
    public static String readString(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] data = new byte[len];
        buffer.get(data);
        return new String(data);
    }
    
    public static void writeString(String str, GatheringByteChannel channel) throws IOException {
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
    public static long getCurrentRollTs(long ts, long rollPeriod) {
        rollPeriod = rollPeriod * 1000;
        ts = ts + localTimezoneOffset;
        long ret = (ts / rollPeriod) * rollPeriod;
        ret = ret - localTimezoneOffset;
        return ret;
    }
    
    /*
     * get the stage roll timestamp, for example
     * now is 16:02, and rollPeroid is 1 hour, then
     * return ts of 15:00;
     */
    public static long getLatestRotateRollTs(long ts, long rollPeriod) {
        return getLatestRotateRollTsUnderTimeBuf(ts, rollPeriod, 0);
    }
    
    /*
     * get the stage roll timestamp under a forward delay, for example
     * timebuf is 5000, now is 15:59:55, and rollPeroid is 1 hour, then
     * return ts of 15:00;
     * timebuf is 5000, now is 15:59:54, and rollPeroid is 1 hour, then
     * return ts of 14:00;
     */
    public static long getLatestRotateRollTsUnderTimeBuf(
            long ts, long rollPeriod, long clockSyncBufMillis) {
        rollPeriod = rollPeriod * 1000;
        ts = ts + localTimezoneOffset;
        long ret = ((ts + clockSyncBufMillis) / rollPeriod -1) * rollPeriod;
        ret = ret - localTimezoneOffset;
        return ret;
    }
    
    public static String getParentAbsolutePath(String absolutePath) {
        return absolutePath.substring(0, absolutePath.lastIndexOf(
                System.getProperty("file.separator")));
    }
    
    public static String formatTs(long ts) {
        return format.format(new Date(ts));
    }
    
    public static String getKey(String content) {
        return content.substring(0, content.indexOf('=') - 1);
    }
    public static String getValue(String content) {
        return content.substring(content.indexOf('=') + 1);
    }

    public static String[] getStringListOfLionValue(String value) {
        if (value == null) {
            return null;
        }
        if (value.charAt(0) != '[' || value.charAt(value.length() - 1) != ']') {
            return null;
        }
        String[] tmp = value.substring(1, value.length() - 1).split(",");
        String[] result = new String[tmp.length];
        for (int i = 0; i < tmp.length; i++) {
            result[i] = tmp[i].substring(1, tmp[i].length() -1 );
        }
        return result;
    }

    public static String[][] getStringMapOfLionValue(String value) {
        if (value == null) {
            return null;
        }
        if (value.charAt(0) != '{' || value.charAt(value.length() - 1) != '}') {
            return null;
        }
        String[] tmp = value.substring(1, value.length() - 1).split(",");
        if (tmp.length == 0) {
            return null;
        }
        String[][] result = new String[tmp.length][2];
        for (int i = 0; i < tmp.length; i++) {
            String[] tmp2 = tmp[i].split(":");
            for (int j = 0; j < 2; j++) {
                result[i][j] = tmp2[j].trim().substring(1, tmp2[j].length() -1);
            }
        }
        return result;
    }

    public static long getCRC32(byte[] data) {
        return getCRC32(data, 0 ,data.length);
    }
    
    public static long getCRC32(byte[] data, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(data, offset, length);
        return crc.getValue();
    }
    
    public static void checkDir(File dir) throws IOException {
        if (!dir.exists()) {
            dir.mkdirs();
        }
        if (!dir.isDirectory()) {
            throw new IOException("file " + dir
                    + " exists, it should be directory");
        }
    }

    public static void rmr(File file) throws IOException {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            
            if (children == null) {
                throw new IOException("error listing directory " + file);
            }
            
            for (File f : children) {
                rmr(f);
            }
            file.delete();
        } else {
            file.delete();
        }
    }

    public static String fromBytes(byte[] b) {
        return fromBytes(b, "UTF-8");
    }

    public static String fromBytes(byte[] b, String encoding) {
        if (b == null) return null;
        try {
            return new String(b, encoding);
        } catch (UnsupportedEncodingException e) {
            return new String(b);
        }
    }

    public static String getHostFromBroker(String brokerString) {
        return brokerString.substring(0, brokerString.lastIndexOf(':'));
    }
    
    public static String getPortFromBroker(String brokerString) {
        return brokerString.substring(brokerString.lastIndexOf(':') + 1);
    }

}

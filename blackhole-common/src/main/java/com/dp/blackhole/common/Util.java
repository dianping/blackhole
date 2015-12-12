package com.dp.blackhole.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SocketChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.network.ByteBufferNonblockingConnection;
import com.dp.blackhole.protocol.control.MessagePB.Message;

public class Util {
    public static final Log LOG = LogFactory.getLog(Util.class);
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static long localTimezoneOffset = TimeZone.getTimeZone("Asia/Shanghai").getRawOffset();
    private static String zkEnv;
    private static int authorizationId;
    
    public static void setZkEnv(String _zkEnv) {
        zkEnv = _zkEnv;
    }
    
    public static void setAuthorizationId(int id) {
        authorizationId = id;
    }
    
    public static InetSocketAddress getRemoteAddr(Socket socket) {
        return (InetSocketAddress) socket.getRemoteSocketAddress();
    }
    
    public static String getRemoteHost(Socket socket) {
      InetSocketAddress remoteAddr= ((InetSocketAddress)socket.getRemoteSocketAddress());
      return remoteAddr.getHostName();
    }
    
    public static String getRemoteHostAndPort(Socket socket) {
        InetSocketAddress remoteAddr= ((InetSocketAddress)socket.getRemoteSocketAddress());
        return remoteAddr.toString();
    }
    
    public static String getLocalHost() {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            String localhost = addr.getHostName();
            if (isNameResolved(addr)) {
                return localhost;
            } else {
                throw new RuntimeException(localhost + " can not be name resolved.");
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static String getLocalHostIP() throws UnknownHostException, SocketException {
        String ip = null;
        Enumeration<NetworkInterface> interfaces  = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface ni = (NetworkInterface) interfaces.nextElement();
            Enumeration<InetAddress> enumIpAddr = ni.getInetAddresses();
            while (enumIpAddr.hasMoreElements()) {
                 InetAddress inetAddress = (InetAddress) enumIpAddr.nextElement();
                 if (!inetAddress.isLoopbackAddress()  
                         && !inetAddress.isLinkLocalAddress() 
                         && inetAddress.isSiteLocalAddress()) {
                     ip = inetAddress.getHostAddress();
                 }
             }
          }
        return ip;
    }
    
    public static boolean isNameResolved(InetAddress address) {
        String hostname = address.getHostName();
        String ip = address.getHostAddress();
        return !hostname.equals(ip);
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
                            + "[_a-z0-9-\\.]*"
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

    public static String getFormatFromPeriod (long period) {
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
    
    public static String getFormatFromPeriodForPath (long period) {
        String format;
        if (period < 86400) {
            format = "yyyy-MM-dd.HH";
        } else {
            format = "yyyy-MM-dd";
        }
        return format;
    }
    
    public static String getRegexFormPeriod(long period) {
        String pattern;
        if (period < 60) {
            pattern = "\\d{4}-\\d{2}-\\d{2}\\.\\d{2}\\.\\d{2}\\.\\d{2}";
        } else if (period < 3600) {
            pattern = "\\d{4}-\\d{2}-\\d{2}\\.\\d{2}\\.\\d{2}";
        } else if (period < 86400) {
            pattern = "\\d{4}-\\d{2}-\\d{2}\\.\\d{2}";
        } else {
            pattern = "\\d{4}-\\d{2}-\\d{2}";
        }
        return pattern;
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
    
    public static byte[] serialize(Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            return bos.toByteArray();
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static Object deserialize(byte[] serialized) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object ret = ois.readObject();
            ois.close();
            return ret;
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        } catch(ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * return the offset of beginning of last line in a file, at which to set file-pointer 
     * @param reader
     * @param fileLength
     * @return return the offset(from 0) of beginning of last line
     * @throws IOException
     */
    public static long seekLastLineHeader(RandomAccessFile reader, long fileLength) throws IOException {
        return seekLastLineHeader(reader, fileLength, 1024 * 8);
    }
    
    public static long seekLastLineHeader(RandomAccessFile reader, long fileLength, int bufSize) throws IOException {
        long headerOffset = 0;
        byte readBuf[] = new byte[bufSize];
        int loop = 0;
        boolean found = false;
        while (!found) {
            loop++;
            long repos = (fileLength - 1) - bufSize * loop; //fileLength - 1 cause offset index begin from 0
            if (repos < 0) {
                //if date is less then 8k or there is not entire line in line, reset to header of file.
                headerOffset = 0;
                break;
            } else {
                reader.seek(repos);
                //prepare to read a buffer to find last \n
                int num = reader.read(readBuf);
                for (int i = num - 1; i >= 0; i--) {
                    final byte ch = readBuf[i];
                    switch (ch) {
                    case '\n':
                        found = true;
                        headerOffset = repos + i + 1;
                        break;
                    default:
                        break;
                    }
                    if (found) {
                        break;
                    }
                }
            }
        }
        reader.seek(headerOffset);
        return headerOffset;
    }
    
    public static long getTS() {
        Date now = new Date();
        return now.getTime();
    }
    
    public static long getTS(int number, TimeUnit unit) {
        return getTS() + unit.toMillis(number);
    }
    
    public static String getTimeString(long ts) {
        SimpleDateFormat printFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        return printFormat.format(new Date(ts));
    }
    
    /*
     * get roll timestamp, for example
     * now is 16:02, and rollPeriod is 1 hour, then
     * return st of 17:00
     */
    public static long getNextRollTs(long ts, long period) {
        period = period * 1000;
        ts = ts + localTimezoneOffset;
        long ret = (ts / period + 1) * period;
        ret = ret - localTimezoneOffset;
        return ret;
    }
    
    /*
     * get the closest step timestamp, for example
     * now is 16:14, and step period is 1 hour, then
     * return ts of 16:00;
     * now is 15:47, and rollPeriod is 1 hour, then
     * return ts of 16:00
     */
    public static long getClosestRollTs(long ts, long period) {
        period = period * 1000;
        ts = ts + localTimezoneOffset;
        long ret;
        if ((ts % period) < (period/2)) {
            ret = (ts / period) * period;
        } else {
            ret = (ts / period + 1) * period;
        }
        ret = ret - localTimezoneOffset;
        return ret;
    }
    
    /*
     * get roll timestamp, for example
     * now is 16:02, and rollPeriod is 1 hour, then
     * return st of 16:00
     */
    public static long getCurrentRollTs(long ts, long rollPeriod) {
        return getCurrentRollTsUnderTimeBuf(ts, rollPeriod, 0);
    }
    
    public static long getCurrentRollTsUnderTimeBuf(
            long ts, long rollPeriod, long clockSyncBufMillis) {
        rollPeriod = rollPeriod * 1000;
        ts = ts + localTimezoneOffset;
        long ret = ((ts + clockSyncBufMillis) / rollPeriod) * rollPeriod;
        ret = ret - localTimezoneOffset;
        return ret;
    }
    
    /*
     * get the stage roll timestamp under a forward delay, for example
     * timebuf is 5000, now is 15:59:55, and rollPeriod is 1 hour, then
     * return ts of 15:00;
     * timebuf is 5000, now is 15:59:54, and rollPeriod is 1 hour, then
     * return ts of 14:00;
     */
    public static long getLatestRollTsUnderTimeBuf(
            long ts, long rollPeriod, long clockSyncBufMillis) {
        rollPeriod = rollPeriod * 1000;
        ts = ts + localTimezoneOffset;
        long ret = ((ts + clockSyncBufMillis) / rollPeriod -1) * rollPeriod;
        ret = ret - localTimezoneOffset;
        return ret;
    }
    
    public static long getCurrentRotationUnderTimeBuf(
            long ts, long rotatePeriod, long clockSyncBufMillis) {
        return getCurrentRollTsUnderTimeBuf(ts, rotatePeriod, clockSyncBufMillis);
    }
    
    public static long getLatestRotateTsAssociateWithRollPeriodUnderTimeBuf(
            long ts, long rollPeriod, long rotatePeriod, long clockSyncBufMillis) {
        return getLatestRollTsUnderTimeBuf(ts, rotatePeriod, clockSyncBufMillis) + (rotatePeriod - rollPeriod) * 1000L;
    }

    public static boolean belongToSameRotate(long rollTs1, long rollTs2, long rotatePeriod) {
        return getCurrentRollTs(rollTs1, rotatePeriod) == getCurrentRollTs(rollTs2, rotatePeriod);
    }
    
    public static boolean isRollConcurrentWithRotate(long currentTs, long rollPeriod, long rotatePeriod) {
        long currentClosestStepTs = Util.getClosestRollTs(currentTs, rollPeriod);
        return (currentClosestStepTs + localTimezoneOffset) % (rotatePeriod * 1000) == 0;
    }

    public static int getMissRotateRollCount(long lastRotateTs, long resumeRollTs, long rotatePeriod) {
        return (int) ((getCurrentRollTs(resumeRollTs, rotatePeriod) - getNextRollTs(lastRotateTs, rotatePeriod))/(rotatePeriod * 1000L));
    }
    
    public static String getParentAbsolutePath(String absolutePath) {
        return absolutePath.substring(0, absolutePath.lastIndexOf(
                System.getProperty("file.separator")));
    }
    
    public static String formatTs(long ts) {
        return format.format(new Date(ts));
    }
    
    public static String formatTs(long ts, long period) {
        return new SimpleDateFormat(getFormatFromPeriod(period)).format(new Date(ts));
    }
    
    public static long parseTs(String timeString, long period) throws ParseException {
        return new SimpleDateFormat(getFormatFromPeriod(period)).parse(timeString).getTime();
    }
    
    public static String getKey(String content) {
        return content.substring(0, content.indexOf('=') - 1);
    }
    public static String getValue(String content) {
        return content.substring(content.indexOf('=') + 1);
    }
    
    public static String generateGetURL(String key) {
        return ParamsKey.LionNode.DEFAULT_LION_HOST +
                ParamsKey.LionNode.LION_GET_PATH +
                generateURIPrefix() +
                "&k=" + key;
    }

    public static String generateSetURL(String key, String value) {
        String encodedValue = "";
        try {
            encodedValue = URLEncoder.encode(value,"UTF-8");
        } catch (UnsupportedEncodingException e) {
        }
        return ParamsKey.LionNode.DEFAULT_LION_HOST +
                ParamsKey.LionNode.LION_SET_PATH +
                generateURIPrefix() +
                "&ef=1" +
                "&k=" + key +
                "&v=" + encodedValue;
    }

    private static String generateURIPrefix() {
        return "?&p=" + ParamsKey.LionNode.LION_PROJECT +
                "&e=" + zkEnv +
                "&id=" + authorizationId;
    }

    public static String[] getStringListOfLionValue(String rawValue) {
        if (rawValue == null) {
            return null;
        }
        String value = rawValue.trim();
        if (value.length() < 2) {
            return null;
        }
        if (value.charAt(0) != '[' || value.charAt(value.length() - 1) != ']') {
            return null;
        }
        if (value.length() == 2) {
            return new String[]{};
        }
        String[] tmp = value.substring(1, value.length() - 1).split(",");
        String[] result = new String[tmp.length];
        for (int i = 0; i < tmp.length; i++) {
            result[i] = tmp[i].trim().substring(1, tmp[i].trim().length() -1 );
        }
        return result;
    }
    
    public static String getStringOfLionValue(String rawValue) {
        if (rawValue == null) {
            return null;
        }
        String[] cmdbApp = getStringListOfLionValue(rawValue);
        if (cmdbApp == null) {
            return rawValue.trim();
        } else {
            return cmdbApp[0];
        }
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
    
    public static int getPortFromBroker(String brokerString) {
        return Integer.parseInt(brokerString.substring(brokerString.lastIndexOf(':') + 1));
    }
    
    public static String toTupleString(Object... args) {
        if (args == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (Object o : args) {
            sb.append(o.toString())
            .append(',');
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append('}');
        return sb.toString();
    }
    
    public static void send(ByteBufferNonblockingConnection connection, Message message) {
        if (connection != null) {
            connection.send(PBwrap.PB2Buf(message));
        } else {
            LOG.info("peer is not connected, message sending abort " + message);
        }
    }
    
    public static String getSource(String agentServer, String instanceId) {
        if (instanceId == null || instanceId.trim().length() == 0) {
            return agentServer;
        } else {
            return agentServer + "#" + instanceId;
        }
    }
    
    public static String getInstanceIdFromSource(String source) {
        String[] splits = source.split("#");
        if (splits.length == 2) {
            return splits[1];
        } else {
            return null;
        }
    }
    
    public static String getHostFromSource(String source) {
        String[] splits = source.split("#");
        return splits[0];
    }
    
    public static String getProducerIdFromPartitionId(String partitionId) {
        return getHostFromSource(partitionId);
    }
    
    /**
     * Returns comma-separated concatenated single String of all strings of the
     * given collection
     */
    public static String printCollection(Collection<String> strings) {
        StringBuilder sb = new StringBuilder(ParamsKey.HTTP.INITIAL_CAPACITY);
        boolean first = true;
        for (String str : strings) {
            if (!first) {
                sb.append(",");
            } else {
                first = false;
            }
            sb.append(str);
        }
        return sb.toString();
    }
    
    public static int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }
    
    public static long parseLong(String value, long defaultValue) {
        try {
            return Long.parseLong(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }
    
    public static boolean parseBoolean(String value, boolean defaultValue) {
        try {
            return Boolean.parseBoolean(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }
    
    public static double parseDouble(String value, double defaultValue) {
        try {
            return Double.parseDouble(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }
    
    public static float parseFloat(String value, float defaultValue) {
        try {
            return Float.parseFloat(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }
        
    public static <T> T newInstance(Class<T> theClass, Object... params) {
        Class<?>[] paramTypeArray = new Class[params.length];
        for (int i = 0; i < params.length; i++) {
            paramTypeArray[i] = params[i].getClass();
        }
        T result;
        try {
            Constructor<T> meth= theClass.getDeclaredConstructor(paramTypeArray);
                meth.setAccessible(true);
            result = meth.newInstance(params);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }
    
    public static void logError(Log log, Throwable t, String... args) {
        StringBuffer sb = new StringBuffer();
        for (String arg : args) {
            sb.append(arg).append(" ");
        }
        if (t == null) {
            log.error(sb.toString());
        } else {
            log.error(sb.toString(), t);
        }
    }
}

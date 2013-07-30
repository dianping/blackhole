package com.dp.blackhole.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.AppRegPB.AppReg;
import com.dp.blackhole.common.AppRollPB.AppRoll;
import com.dp.blackhole.common.MessagePB.Message;
import com.dp.blackhole.conf.CollectorConfigurationConstants;
import com.dp.blackhole.conf.ConfigKeeper;

public class Util {
    private static final Log LOG = LogFactory.getLog(Util.class);

    //-----------------------------collecotr------------------------------------//
    public static String getHDFSPathByIdent(String appName, String appHost, final String fileSuffix) {
        // HDFS file: basePath / appName / filePerfix[0] / filePerfix[1] / appHost_appName_fileSuffix
        // HDFS file: ..base.. / access  / 2013-07-11    /        12     / hostname_access_2013-07-11.12
        // HDFS file: ..base.. / access  / 2013-07-11    /                 hostname_access_2013-07-11
        // filePerfix: 2013-07-11.12
        // filePerfix: 2013-07-11
        String basePath = ConfigKeeper.configMap.get(appName)
                .getString(CollectorConfigurationConstants.BASE_HDFS_PATH);
        String path = basePath + "/" + appName;
        String[] fileSuffixs = fileSuffix.split("\\.");
        switch (fileSuffixs.length) {
        case 2: //2013-07-11.12 
            path += ("/" + fileSuffixs[0] + "/" + fileSuffixs[1]);
            break;
        case 1: //appName.2013-07-11
            path += ("/" + fileSuffixs[0]);
            break;

        default:
            LOG.warn("fileSuffix " + fileSuffix + " is illegal");
            break;
        }
        path += ("/" + appHost + "_" + appName + "_" + fileSuffix);
        return path;    //    ..base../appName/2013-07-11/12/appName.2013-07-11.12
    }

    //---------------------------app--------------------------------------//
    public static String getRollIdentByTime(Date date, int interval) {
        //first version, we use 60 min (1 hour) as fixed interval
        SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd.hh:00:00");
        return dayFormat.format(getOneHoursAgoTime(date));
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

    public static String getFormatByUnit(String unit) {
        if (unit.equalsIgnoreCase("hour")) {
            return "yyyy-MM-dd.hh";
        } else if (unit.equalsIgnoreCase("day")) {
            return "yyyy-MM-dd";
        } else if (unit.equalsIgnoreCase("minute")) {
            return "yyyy-MM-dd.hh:mm";
        } else {
            LOG.warn("Period unit is not valid, use hour for default.");
            return "yyyy-MM-dd.hh";
        }
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
}

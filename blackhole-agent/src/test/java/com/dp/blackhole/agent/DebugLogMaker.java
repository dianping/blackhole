package com.dp.blackhole.agent;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Enumeration;
import java.util.Random;
import java.util.Scanner;

import org.apache.log4j.Appender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class DebugLogMaker {
    static Logger LOG;
    
    public static String create(){
        String str = null;
        int hightPos, lowPos; // 定义高低位
        Random random = new Random();
        hightPos = (176 + Math.abs(random.nextInt(39)));//获取高位值
        lowPos = (161 + Math.abs(random.nextInt(93)));//获取低位值
        byte[] b = new byte[2];
        b[0] = (new Integer(hightPos).byteValue());
        b[1] = (new Integer(lowPos).byteValue());
        try {
            str = new String(b, "gbk");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }//转成中文
        return str;
    }
    
    public static String getRandomString(int length) { //length表示生成字符串的长度
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";   //生成字符串从此序列中取
        Random random = new Random();   
        StringBuffer sb = new StringBuffer();   
        for (int i = 0; i < length; i++) {   
            int number = random.nextInt(base.length());   
            sb.append(base.charAt(number));   
        }   
        return sb.toString();   
     }
    
    public static long getTS() {
        Date now = new Date();
        return now.getTime();
    }
    
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Default output file is /tmp/1.log, type 'f' to change, just enter to skip:");
        Scanner input = new Scanner(System.in);
        String flag = input.nextLine();
        String outputfile = "/tmp/1.log";
        if (flag.equals("f")) {
            System.out.println("New output file is:");
            outputfile = input.nextLine(); 
        }
        System.out.println("Using " + outputfile);
        
        System.out.println("Default rotation period is one hour, type 'm' to use minute rotation, just enter to skip:");
        String dataPattern = "'.'yyyy-MM-dd.HH";
        flag = input.nextLine();
        if (flag.equals("m")) {
            dataPattern = "'.'yyyy-MM-dd.HH.mm";
        }
        System.out.println("Using " + dataPattern);
        
        System.out.println("Default line has 100 characters, type other number to change to, just enter to skip:");
        int lineNum = 100;
        flag = input.nextLine();
        if (flag.isEmpty()) {
        } else {
            lineNum = Integer.parseInt(flag);
        }
        System.out.println("Using " + lineNum + " characters a line");
        
        System.out.println("Default line interval is 1000 milliseconds, type other number to change to, just enter to skip:");
        int interval = 1000;
        flag = input.nextLine();
        if (flag.isEmpty()) {
        } else {
            interval = Integer.parseInt(flag);
        }
        System.out.println("Using " + interval + " milliseconds interval between two lines");
        input.close();
        System.out.println("Begin to log, " + getRandomString(10));
        DailyRollingFileAppender appender = new DailyRollingFileAppender();
        appender.setLayout(new PatternLayout("%m%n"));
        appender.setFile(outputfile);
        appender.setDatePattern(dataPattern);
        appender.setThreshold(Level.DEBUG);
        appender.activateOptions();
        Logger.getRootLogger().addAppender(appender);
        LOG = Logger.getLogger(DebugLogMaker.class);
        
        Enumeration<?> e = Logger.getRootLogger().getAllAppenders();
        while ( e.hasMoreElements() ){
          Appender app = (Appender)e.nextElement();
          if ( app instanceof FileAppender ){
            System.out.println("File: " + ((FileAppender)app).getFile());
          }
        }

        int i =0;
        while (true) {
            StringBuffer sb = new StringBuffer();
            i++;
            sb.append(i);
            sb.append(' ');
            sb.append(Long.toString(DebugLogMaker.getTS()));
            sb.append(' ');
            for (int j = 0; j < lineNum; j++) {
                sb.append(getRandomString(1));
            }
            LOG.info(sb.toString());
            Thread.sleep(interval);
        }
    }
}


package com.dp.blackhole.simutil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.collectornode.RollIdent;

public class Util {
    private static final Log LOG = LogFactory.getLog(Util.class);
    public static final String HOSTNAME = "localhost";
    public static final String SCHEMA = "file://";
    public static final String BASE_PATH = "/tmp/";
    public static final String BASE_HDFS_PATH = SCHEMA + BASE_PATH;
    public static final String FILE_SUFFIX = "2013-01-01.15";
    public static long rollTS = 1357023691855l;
    public static void main(String[] args) {
        long ts = com.dp.blackhole.common.Util.getRollTs(rollTS, 3600l);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd.HH");
        System.out.println(format.format(ts));
    }
    public static final int PORT = 40000;
    public static final String expected = " 0f j2390jr092jf2f02jf02qjdf2-3j0 fiopwqejfjwffhg5_p    <end";
    public static final int offset = 100;
    public static final int MAX_LINE = 9;
    
    public static void initEnv() {
        
    }
    
    public static RollIdent getRollIdent(String appName) {
        RollIdent rollIdent = new RollIdent();
        rollIdent.app = appName;
        rollIdent.period = 3600;
        rollIdent.source = HOSTNAME;
        rollIdent.ts = rollTS;
        return rollIdent;
    }
    
    public static File createTmpFile(String MAGIC, String expected) 
            throws IOException, FileNotFoundException {
        String string = 
                "begin>    owefoq jfojnofownfowofnownefowoefojweofjwosfnvvoco\n" +
                "jlsdfpasjdfaopsdpfaskdfkpasdkpfkasdfas     100>     jcsopdnvon\n" +
                "vononoifjopwejf opwjfiop jpwj fopqwejfop qjfopiqjqertgbrtg\n" +
                "aspd jfoiasj df ioajsiodf asj fasof jasdopjf pasfj asopfjo\n" +
                "rtgrtghrthrthrthrhrthtrp sjfop asdj fopasj fopsfjopsjf wef\n" +
                "j faiosjf opwqejo fjopwej faeopsf jopawefj opsjf opsafj ao\n" +
                " wopejf opwj efopqwj epo fjwopefj pwef opw ejfopwj efopwf \n" +
                "3 wjopef joiqwf io j 9049 fj2490r 0pjfioj fioj qiowegio f \n" +
                " f90fj 9034u j90 jgioqpwejf iopwe jfopqwefj opewji fopq934\n" +
                expected + "\n";
        //build a app log
        File file = File.createTempFile(MAGIC, null);
        LOG.info("create tmp file " + file);
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file)));
        writer.write(string);
        writer.close();
        return file;
    }
    
    public static File createBrokenTmpFile(String MAGIC, String expected) 
            throws IOException, FileNotFoundException {
        String string = 
                "begin>    owefoq jfojnofownfowofnownefowoefojweofjwosfnvvoco\n" +
                "jlsdfpasjdfaopsdpfaskdfkpasdkpfkasdfas     100>     jcsopdnvon\n" +
                "vononoifjopwejf opwjfiop jpwj fopqwejfop qjfopiqjqertgbrtg\n" +
                "aspd jfoiasj df ioajsiodf asj fasof jasdopjf pasfj asopfjo\n" +
                "rtgrtghrthrthrthrhrthtrp sjfop asdj fopasj fopsfjopsjf wef\n" +
                "j faiosjf opwqejo fjopwej faeopsf jopawefj opsjf opsafj ao\n" ;
        //build a app log
        File file = File.createTempFile(MAGIC, null);
        LOG.info("create tmp broken file " + file);
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file)));
        writer.write(string);
        writer.close();
        return file;
    }
    
    public static void deleteTmpFile(String MAGIC) {
        File dir = new File("/tmp");
        for (File file : dir.listFiles()) {
            if (file.getName().startsWith(MAGIC)) {
                LOG.info("delete tmp file " + file);
                file.delete();
            }
        }
    }
    
    public static void convertToGZIP(File file) 
            throws FileNotFoundException, IOException {
        byte[] buf = new byte[8196];
        BufferedInputStream bin= new BufferedInputStream(new FileInputStream(file));
        File gzFile = new File(file.getAbsoluteFile() + ".tmp");
        GZIPOutputStream gout = new GZIPOutputStream(new FileOutputStream(gzFile));
        int len;
        while ((len = bin.read(buf)) != -1) {
            gout.write(buf, 0, len);
        }
        gout.close();
        bin.close();
        gzFile.renameTo(file.getAbsoluteFile());
        file = gzFile;
    }
    
    public static File convertToNomal(File file) 
            throws FileNotFoundException, IOException {
        byte[] buf = new byte[8196];
        GZIPInputStream gin = new GZIPInputStream(new FileInputStream(file));
        File nomalFile = new File(file.getAbsolutePath() + ".nomal");
        BufferedOutputStream bout= new BufferedOutputStream(
                new FileOutputStream(nomalFile));
        int len;
        while ((len = gin.read(buf)) != -1) {
            bout.write(buf, 0, len);
        }
        bout.close();
        gin.close();
        file.delete();
        file = nomalFile;
        return file;
    }
}

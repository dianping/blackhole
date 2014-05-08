package com.dp.blackhole.agent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.agent.Agent;
import com.dp.blackhole.common.Util;

public class SimAppnode extends Agent{
    private static final Log LOG = LogFactory.getLog(SimAppnode.class);
    public static final int COLPORT = 11113;
    public static String HOSTNAME;
    public static long rollTS = 1357023691855l;
    public static final String SCHEMA = "file://";
    public static final String BASE_PATH = "/tmp/hdfs/";
    public static final String FILE_SUFFIX = "2013-01-01.15";
    public static final String expected = " 0f j2390jr092jf2f02jf02qjdf2-3j0 fiopwqejfjwffhg5_p    <end";
    public static final String TEST_ROLL_FILE = "/tmp/rollfile";
    
    static {
        try {
            HOSTNAME = Util.getLocalHost();
        } catch (UnknownHostException e) {
        }
    }
    
    public SimAppnode() {
        super();
        super.processor = new AgentProcessor();
    }
    
    @Override
    public String getHost() {
        try {
            return Util.getLocalHost();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void reportFailure(String app, String appHost, long ts) {
        LOG.debug("APP: " + app + ", APP HOST: " + appHost + "ts: " + ts);
    }
    
    public void reportUnrecoverable(String appName, String appHost, long ts) {
        LOG.debug("APP: " + appName + ", APP HOST: " + appHost + "roll ts: " + ts);
    }
    
    public static void deleteTmpFile(String MAGIC) {
        File dir = new File("/tmp");
        for (File file : dir.listFiles()) {
            if (file.getName().contains(MAGIC)) {
                LOG.debug("delete tmp file " + file);
                if (file.isDirectory()) {
                    deleteInnerFile(file.listFiles());
                    file.delete();
                } else {
                    file.delete();
                }
            }
        }
    }
    
    private static void deleteInnerFile(File[] files) {
        for (File file : files) {
            if (file.isDirectory()) {
                deleteInnerFile(file.listFiles());
                file.delete();
            } else {
                file.delete();
            }
        }
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
        File file = new File("/tmp/" + MAGIC);
        file.createNewFile();
        LOG.debug("create tmp file " + file);
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file)));
        writer.write(string);
        writer.close();
        return file;
    }
}

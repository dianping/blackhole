package com.dp.blackhole.agent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.agent.Agent;
import com.dp.blackhole.agent.AgentMeta.TopicId;
import com.dp.blackhole.common.Util;

public class SimAgent extends Agent{
    private static final Log LOG = LogFactory.getLog(SimAgent.class);
    public static final int COLPORT = 11113;
    public static String HOSTNAME;
    public static long rollTS = 1357023691855l;
    public static final String SCHEMA = "file://";
    public static final String BASE_PATH = "/tmp/hdfs/";
    public static final String FILE_SUFFIX = "2013-01-01.15";
    public static final String expected = " 0f j2390jr092jf2f02jf02qjdf2-3j0 fiopwqejfjwffhg5_p    <end";
    public static final String TEST_ROLL_FILE = "/tmp/rollfile";
    
    static {
        HOSTNAME = Util.getLocalHost();
    }
    
    public SimAgent() {
        super();
        super.processor = new AgentProcessor();
        super.setHost(HOSTNAME);
    }
    
//    @Override
//    public static String getHost() {
//        try {
//            return Util.getLocalHost();
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
    
    @Override
    public void reportLogReaderFailure(TopicId topicId, String appHost, long ts) {
        LOG.debug(topicId + " log reader fail, APP HOST: " + appHost + "ts: " + ts);
    }
    
    @Override
    public void reportRemoteSenderFailure(TopicId topicId, String appHost, long ts, int delay) {
        LOG.debug(topicId + " remote sender fail, APP HOST: " + appHost + "ts: " + ts + "delay: " + delay);
    }
    
    @Override
    public void reportUnrecoverable(TopicId topicId, String appHost, final long period, long ts, boolean isFinal, boolean isPersist) {
        LOG.debug(topicId + ", APP HOST: " + appHost  + ", period: " + period + ", roll ts: " + ts + ", final:" + isFinal + ", persist:" + isPersist);
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
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    deleteInnerFile(files[i].listFiles());
                    files[i].delete();
                } else {
                    files[i].delete();
                }
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

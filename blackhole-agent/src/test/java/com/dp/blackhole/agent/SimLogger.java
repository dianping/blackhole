package com.dp.blackhole.agent;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SimLogger implements Runnable {
    private static final Log LOG = LogFactory.getLog(SimLogger.class);
    public static final String TEST_ROLL_FILE_NAME = "rollfile";
    public static final String TEST_ROLL_FILE = "/tmp/" + TEST_ROLL_FILE_NAME;
    private long delay;
    private OutputStreamWriter writer;
    private static final int WORD_COUNT_PER_LINE = 256;
    private ArrayList<String> verifyLines = new ArrayList<String>();
    
    public ArrayList<String> getVerifyLines() {
        return verifyLines;
    }

    public SimLogger(long delay) {
        this.delay = delay;
    }

    @Override
    public void run() {
        File file = new File(TEST_ROLL_FILE);
        File target;
        boolean renameSucceeded = true;
        try {
            int i = 0;
            int j = 0;
            while (!Thread.interrupted()) {
                if (i%10 == 0) {
                    if (file.exists()) {
                        target = new File(TEST_ROLL_FILE + '.' + (j++));
                        LOG.debug("Renaming file " + file + " to " + target);
                        renameSucceeded = file.renameTo(target);
                        writer.close();
                    }
                }
                if (renameSucceeded) {
                    renameSucceeded = false;
                    writer = new OutputStreamWriter(new FileOutputStream(file, true));
                }
                StringBuffer sb = new StringBuffer();
                sb.append(i);
                sb.append(" : ");
                for (int k = 0; k < WORD_COUNT_PER_LINE; k++) {
                    sb.append(create());
                }
                verifyLines.add(sb.toString());
                sb.append('\n');
                writer.write(sb.toString());
                writer.flush();
                i++;
                Thread.sleep(delay);
            }
        } catch (InterruptedException e) {
            //stop
            File dir = new File("/tmp");
            for (File del : dir.listFiles()) {
                if (del.getName().startsWith("rollfile")) {
                    del.delete();
                }
            }
        } catch (FileNotFoundException e) {
            LOG.debug("OOPS", e);
        } catch (IOException e) {
            LOG.debug("OOPS", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
    
    public String create() {
        String str = null;
        int hightPos, lowPos;
        Random random = new Random();
        hightPos = (176 + Math.abs(random.nextInt(39)));
        lowPos = (161 + Math.abs(random.nextInt(93)));
        byte[] b = new byte[2];
        b[0] = (new Integer(hightPos).byteValue());
        b[1] = (new Integer(lowPos).byteValue());
        try {
            str = new String(b, "gbk");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return str;
    }
}

package com.dp.blackhole.broker.ftp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FTPConfigrationLoader implements Runnable {
    private static final Log LOG = LogFactory.getLog(FTPConfigrationLoader.class);
    private static Map<String, FTPConfigration> transferTopics = new HashMap<String, FTPConfigration>();
    
    private long interval;
    private boolean running;
    
    public FTPConfigrationLoader(long interval) {
        this.interval = interval;
        this.running = true;
    }
    
    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public static FTPConfigration getFTPConfigration(String topic) {
        synchronized (transferTopics) {
            return transferTopics.get(topic);
        }
    }

    @Override
    public void run() {
        Properties prop = new Properties();
        while (running) {
            try {
                reloadConfigration(prop);
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
                running = false;
            } catch (Throwable t) {
                LOG.error("Oops, got an exception, FTP config loader will stop.", t);
            }
        }
    }
    
    private void reloadConfigration(Properties prop) throws IOException {
        prop.load(ClassLoader.getSystemResourceAsStream("ftp.properties"));
        String url = prop.getProperty("ftp.url");
        int port = Integer.parseInt(prop.getProperty("ftp.port"));
        String username = prop.getProperty("ftp.username");
        String password = prop.getProperty("ftp.password");
        String rootDir = prop.getProperty("ftp.rootdir");
        FTPConfigration configration = new FTPConfigration(url, port, username, password, rootDir);
        String ftpTopicStr = prop.getProperty("ftp.topic");
        if (ftpTopicStr == null) {
            return;
        }
        String[] ftpTopics = ftpTopicStr.split(",");
        synchronized (transferTopics) {
            transferTopics.clear();
            for (int i = 0; i < ftpTopics.length; i++) {
                transferTopics.put(ftpTopics[i], configration);
            }
        }
    }
}

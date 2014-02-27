package com.dp.blackhole.check;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

public class CheckDone implements Runnable{
    private final static Log LOG = LogFactory.getLog(CheckDone.class);
    
    private RollIdent ident;
    private boolean firstDeploy;
    private boolean tried = false;
    public CheckDone (RollIdent ident) {
        this.ident = ident;
        this.firstDeploy = ident.firstDeploy;
    }
    
    @Override
    public String toString() {
        return "CheckDone [ident=" + ident.toString() + ", firstDeploy=" + firstDeploy + "]";
    }

    @Override
    public void run() {
        Calendar calendar = Calendar.getInstance();
        long nowTS = calendar.getTimeInMillis();
        List<String> attemptSource = new ArrayList<String>();
        while (ident.ts <= Util.getPrevWholeTs(nowTS, ident.period)) {
            LOG.info("Handling app " + ident.app + ", roll ts " + new Date(ident.ts).toString());
            attemptSource.clear();
            if (!wasDone(ident)) {
                Path expectedFile = null;
                for(String source : ident.sources) {
                    expectedFile = getRollHdfsPath(ident, source);
                    if (!retryExists(expectedFile)) {
                        LOG.info("File " + expectedFile + " not ready.");
                        attemptSource.add(source);
                    }
                }
                if (attemptSource.isEmpty()) { //all file ready
                    if (expectedFile != null) {
                        if (!retryTouch(expectedFile.getParent())) {
                            LOG.error("Alarm, failed to touch a done file. " +
                            		"Try in next check cycle. " +
                            		"If you see this message for the second time, " +
                            		"please find out why.");
                            break;
                        }
                        firstDeploy = false;
                    } else {
                        LOG.fatal("expectedFile is null. It should not be happen.");
                    }
                } else {
                    if (firstDeploy) { // first deploy , ident.ts is a mock ts
                        ident.ts = Util.getNextWholeTs(ident.ts, ident.period);
                        tried = false;
                        break;
                    }
                    if (calendar.get(Calendar.MINUTE) >= alartTime) {
                        if (!tried) {
                            LOG.warn("Too long to finish. Attempt to recovery using blackhole cli once.");
                            Runtime runtime = Runtime.getRuntime();
                            tried = true;
                            for (String source : attemptSource) {
                                String[] cmdarray = new String[3];
                                cmdarray[0] = "sh";
                                cmdarray[1] = blackholeBinPath + "/cli.sh";
                                cmdarray[2] = "auto recovery " + ident.app + " " + source + " " + ident.ts;
                                try {
                                    LOG.info("run command: " + cmdarray[0] + " " + cmdarray[1] + " " + cmdarray[2]);
                                    runtime.exec(cmdarray);
                                } catch (IOException e1) {
                                    LOG.error("Alarm, recovery attempt fail.");
                                }
                                try {
                                    Thread.sleep(20000);
                                } catch (InterruptedException e) {
                                    LOG.error(e.getMessage());
                                }
                            }
                            break;
                        } else {
                            LOG.error("Alarm, auto recovery for " + ident.app + " may be unsuccessful. Please check it out.");
                        }
                    } else {
                        break;
                    }
                }
            }
            ident.ts = Util.getNextWholeTs(ident.ts, ident.period);
            tried = false;
        }
    }
    
    public boolean wasDone (RollIdent ident) {
        String format  = Util.getFormatFromPeroid(ident.period);
        Date roll = new Date(ident.ts);
        SimpleDateFormat dm= new SimpleDateFormat(format);
        Path done =  new Path(hdfsbasedir + '/' + ident.app + '/' + 
                Util.getDatepathbyFormat(dm.format(roll)) + DONE_FLAG);
        if (retryExists(done)) {
            return true;
        } else {
            Path succ =  new Path(hdfsbasedir + '/' + ident.app + '/' + 
                    Util.getDatepathbyFormat(dm.format(roll)) + successprefix + dm.format(roll));
            return retryExists(succ);
        }
    }
    /*
     * Path format:
     * hdfsbasedir/appname/2013-11-01/14/08/machine01@appname_2013-11-01.14.08.gz.tmp
     * hdfsbasedir/appname/2013-11-01/14/08/machine02@appname_2013-11-01.14.08.gz.tmp
     */
    public Path getRollHdfsPath (RollIdent ident, String source) {
        String format  = Util.getFormatFromPeroid(ident.period);
        Date roll = new Date(ident.ts);
        SimpleDateFormat dm= new SimpleDateFormat(format);
        return new Path(hdfsbasedir + '/' + ident.app + '/' + Util.getDatepathbyFormat(dm.format(roll)) + 
                    source + '@' + ident.app + "_" + dm.format(roll) + hdfsfilesuffix);
    }
    
    public boolean retryExists(Path expected) {
        for (int i = 0; i < REPEATE; i++) {
            try {
                return fs.exists(expected);
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
    
    public boolean retryTouch(Path parentPath) {
        FSDataOutputStream out = null;
        Path doneFile = new Path(parentPath, DONE_FLAG);
        for (int i = 0; i < REPEATE; i++) {
            try {
                out = fs.create(doneFile);
                return true;
            } catch (IOException e) {
            } finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException e) {
                        LOG.warn("Close hdfs out put stream fail!", e);
                    }
                }
            }
            try {
                Thread.sleep(RETRY_SLEEP_TIME);
            } catch (InterruptedException ex) {
                return false;
            }
        }
        return false;
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            init();
            for (RollIdent ident : rollIdents) {
                CheckDone checker = new CheckDone(ident);
                LOG.info("create a checkdone thread " + checker.toString());
                checkerThreadPool.scheduleWithFixedDelay(checker, 0, checkperiod, TimeUnit.SECONDS);
            }
        } catch (FileNotFoundException e) {
            LOG.error("Oops, got an exception.", e);
        } catch (NumberFormatException e) {
            LOG.error("Oops, got an exception.", e);
        } catch (IOException e) {
            LOG.error("Oops, got an exception.", e);
        }
    }

    static void init() throws FileNotFoundException, NumberFormatException, IOException {
        Properties prop = new Properties();
        prop.load(new FileReader(new File("checkdone.properties")));
        alartTime = Integer.parseInt(prop.getProperty("ALARM_TIME"));
        successprefix = prop.getProperty("SUCCESS_PREFIX", "_SUCCESS.");
        hdfsbasedir = prop.getProperty("HDFS_BASEDIR");
        if (hdfsbasedir.endsWith("/")) {
            hdfsbasedir = hdfsbasedir.substring(0, hdfsbasedir.length() - 1);
        }
        hdfsfilesuffix = prop.getProperty("HDFS_FILE_SUFFIX");
        checkperiod = Long.parseLong(prop.getProperty("CHECK_PERIOD", "300"));
        fillRollIdent(prop);
        String keytab = prop.getProperty("KEYTAB_FILE");
        String namenodePrincipal = prop.getProperty("NAMENODE.PRINCIPAL");
        String principal = prop.getProperty("PRINCIPAL");
        blackholeBinPath = prop.getProperty("BLACKHOLE_BIN");
        if (blackholeBinPath.endsWith("/")) {
            blackholeBinPath = blackholeBinPath.substring(0, blackholeBinPath.length() - 1);
        }
        Configuration conf = new Configuration();
        conf.set("checkdone.keytab", keytab);
        conf.set("dfs.namenode.kerberos.principal", namenodePrincipal);
        conf.set("checkdone.principal", principal);
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        SecurityUtil.login(conf, "checkdone.keytab", "checkdone.principal");
        fs = (new Path(hdfsbasedir)).getFileSystem(conf);
        LOG.info("Create thread pool");
        checkerThreadPool = Executors.newScheduledThreadPool(Integer.parseInt(prop.getProperty("MAX_THREAD_NUM", "10")));
    }

    private static void fillRollIdent(Properties prop) {
        String apps = prop.getProperty("APPS");
        String[] appArray = apps.split(",");
        rollIdents = new ArrayList<RollIdent>();
        for (int i = 0; i < appArray.length; i++) {
            String appName;
            if ((appName = appArray[i].trim()).length() == 0) {
                continue;
            }
            RollIdent rollIdent = new RollIdent();
            rollIdent.app = appName;
            String[] hosts = prop.getProperty(appName + ".APP_HOSTS").split(",");
            List<String> sources = new ArrayList<String>();
            for (int j = 0; j < hosts.length; j++) {
                String host;
                if ((host = hosts[j].trim()).length() == 0) {
                    continue;
                }
                sources.add(host);
            }
            if (sources.isEmpty()) {
                LOG.error("source hosts are all miss.");
                System.exit(0);
            }
            rollIdent.sources = sources;
            rollIdent.period = Long.parseLong(prop.getProperty(appName + ".ROLL_PERIOD"));
            rollIdent.ts = Long.parseLong(prop.getProperty(appName + ".BEGIN_TS", "1356969600000"));
            rollIdent.firstDeploy = Boolean.parseBoolean(prop.getProperty(appName + ".FIRST_DEPLOY", "false"));
            rollIdents.add(rollIdent);
        }
    }
    
    private static int alartTime;
    private static final String DONE_FLAG = "_done";
    private static String successprefix;
    private static String hdfsbasedir;
    private static String hdfsfilesuffix;
    private static String blackholeBinPath;
    private static FileSystem fs;
    private static final int REPEATE = 3;
    private static final int RETRY_SLEEP_TIME = 1000;
    private static long checkperiod;
    private static ScheduledExecutorService checkerThreadPool;
    private static List<RollIdent> rollIdents;
}

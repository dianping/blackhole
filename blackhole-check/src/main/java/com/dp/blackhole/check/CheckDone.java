package com.dp.blackhole.check;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

public class CheckDone implements Runnable{
    private final static Log LOG = LogFactory.getLog(CheckDone.class);

    private final RollIdent ident;
    private long lastModify;
    private TimeChecker timeChecker;
    public CheckDone (RollIdent ident, TimeChecker timeChecker) {
        this.ident = ident;
        this.timeChecker = timeChecker;
        this.lastModify = lastModifyTime;
    }
    
    @Override
    public String toString() {
        return "CheckDone [ident=" + ident.toString() + ", firstDeploy=" + ident.firstDeploy + "]";
    }

    @Override
    public void run() {
        File confFile = new File("checkdone.properties");
        long lastModify = confFile.lastModified();
        if (this.lastModify != lastModify) {
            this.lastModify = lastModify;
            //reload sources
            Properties prop = new Properties();
            try {
                prop.load(new FileReader(confFile));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            ident.timeout = Integer.parseInt(prop.getProperty(ident.app + ".TIMEOUT_MINUTE", "-1"));
            long newBeginTs = Long.parseLong(prop.getProperty(ident.app + ".BEGIN_TS"));
            if (ident.ts != newBeginTs) {
                ident.ts = newBeginTs;
                String[] hosts = prop.getProperty(ident.app + ".APP_HOSTS").split(",");
                List<String> sources = new ArrayList<String>();
                for (int j = 0; j < hosts.length; j++) {
                    String host;
                    if ((host = hosts[j].trim()).length() == 0) {
                        continue;
                    }
                    sources.add(host);
                }
                ident.sources.clear();
                ident.sources = sources;
            }
        }
        Calendar calendar = Calendar.getInstance();
        long nowTS = calendar.getTimeInMillis();
        List<String> attemptSource = new ArrayList<String>();
        while (ident.ts <= Util.getPrevWholeTs(nowTS, ident.period)) {
            LOG.info("Try to handle [" + ident.app + ":" + Util.format.format(new Date(ident.ts)) + "]");
            attemptSource.clear();
            if (!Util.wasDone(ident, ident.ts)) {
                Path expectedFile = null;
                for(String source : ident.sources) {
                    expectedFile = Util.getRollHdfsPath(ident, source);
                    if (!Util.retryExists(expectedFile)) {
                        LOG.info("File " + expectedFile + " not ready.");
                        attemptSource.add(source);
                    }
                }
                if (attemptSource.isEmpty()) { //all file ready
                    if (expectedFile != null) {
                        if (!Util.retryTouch(expectedFile.getParent(), Util.DONE_FLAG)) {
                            LOG.error("Alarm, failed to touch a done file. " +
                                    "Try in next check cycle. " +
                                    "If you see this message for the second time, " +
                                    "please find out why.");
                            break;
                        } else {
                            LOG.info("[" + ident.app + ":" + Util.format.format(new Date(ident.ts)) + "]===>Done!");
                        }
                        ident.firstDeploy = false;
                    } else {
                        LOG.fatal("expectedFile is null. It should not be happen.");
                    }
                } else {
                    if (ident.firstDeploy) { // first deploy , ident.ts is a mock ts
                        ident.ts = Util.getNextWholeTs(ident.ts, ident.period);
                        break;
                    }
                    if (ident.timeout > 0 && ident.timeout < 60 && calendar.get(Calendar.MINUTE) >= ident.timeout) {
                        if (expectedFile != null) {
                            if (!Util.retryTouch(expectedFile.getParent(), Util.TIMEOUT_FLAG)) {
                                LOG.error("Alarm, failed to touch a TIMEOUT_FLAG file. " +
                                        "Try in next check cycle. " +
                                        "If you see this message for the second time, " +
                                        "please find out why.");
                                break;
                            } else {
                                LOG.info("[" + ident.app + ":" + Util.format.format(new Date(ident.ts)) + "]===>Timeout!");
                            }
                            ident.firstDeploy = false;
                        } else {
                            LOG.fatal("expectedFile is null. It should not be happen.");
                        }
                    } else if (calendar.get(Calendar.MINUTE) >= alartTime) {
                        LOG.error("Alarm, [" + ident.app + ":" + Util.format.format(new Date(ident.ts)) + " unfinished, add to TimeChecker.");
                        timeChecker.registerTimeChecker(ident, ident.ts);
                    } else {
                        break;
                    }
                }
            } else {
                LOG.info("[" + ident.app + ":" + Util.format.format(new Date(ident.ts)) + "]===>Already Done!");
            }
            ident.ts = Util.getNextWholeTs(ident.ts, ident.period);
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            init();
            TimeChecker timeChecker = new TimeChecker(sleepDuration);
            for (RollIdent ident : rollIdents) {
                CheckDone checker = new CheckDone(ident, timeChecker);
                LOG.info("create a checkdone thread " + checker.toString());
                checkerThreadPool.scheduleWithFixedDelay(checker, 0, checkperiod, TimeUnit.SECONDS);
            }
            LOG.info("Start the Time Checker per " + sleepDuration + "millis.");
            timeChecker.start();
        } catch (FileNotFoundException e) {
            LOG.error("Oops, got an exception.", e);
        } catch (NumberFormatException e) {
            LOG.error("Oops, got an exception.", e);
        } catch (IOException e) {
            LOG.error("Oops, got an exception.", e);
        }
    }

    static void init() throws FileNotFoundException, NumberFormatException, IOException {
        File confFile = new File("checkdone.properties");
        lastModifyTime = confFile.lastModified();
        Properties prop = new Properties();
        prop.load(new FileReader(confFile));
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
        sleepDuration = Long.parseLong(prop.getProperty("TIMECHECKER_PROID", "60000"));
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
                LOG.error("Alarm, source hosts are all miss.");
                System.exit(0);
            }
            rollIdent.sources = sources;
            rollIdent.period = Long.parseLong(prop.getProperty(appName + ".ROLL_PERIOD"));
            rollIdent.ts = Long.parseLong(prop.getProperty(appName + ".BEGIN_TS", "1356969600000"));
            rollIdent.firstDeploy = Boolean.parseBoolean(prop.getProperty(appName + ".FIRST_DEPLOY", "false"));
            rollIdent.timeout = Integer.parseInt(prop.getProperty(appName + ".TIMEOUT_MINUTE", "-1"));
            rollIdents.add(rollIdent);
        }
    }

    public static FileSystem fs;
    public static String successprefix;
    public static String hdfsbasedir;
    public static String hdfsfilesuffix;
    public static String hdfsHiddenfileprefix = "_";
    private static int alartTime;
    private static long checkperiod;
    private static ScheduledExecutorService checkerThreadPool;
    private static List<RollIdent> rollIdents;
    private static long lastModifyTime;
    private static long sleepDuration;
}

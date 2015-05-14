package com.dp.blackhole.check;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dianping.lion.client.LionException;

public class CheckDone implements Runnable{
    private final static Log LOG = LogFactory.getLog(CheckDone.class);
    private RollIdent ident;
   
    public CheckDone (RollIdent ident) {
        this.ident = ident;
    }
    
    @Override
    public String toString() {
        return "CheckDone [ident=" + ident.toString()+"]";
    }

    @Override
    public void run() {
        //pass blacklist
        if (lionConfChange.getAppBlacklist().contains(ident.topic)) {
            return;
        }
        //reload sources
        List<String> kvmSources = lionConfChange.getAppToHosts().get(ident.topic);
        if (kvmSources == null) {
            kvmSources = new ArrayList<String>();
        }
        ident.kvmSources = kvmSources;
        
        if (ident.paasSources == null) {
            ident.paasSources = new ArrayList<String>();
        }
        if(kvmSources.isEmpty() && ident.paasSources.isEmpty()) {
            LOG.warn("source hosts are all miss for " + ident.topic);
        }
        
        Calendar calendar = Calendar.getInstance();
        long nowTS = calendar.getTimeInMillis();
        List<String> attemptSource = new ArrayList<String>();
        while (ident.ts <= Util.getPrevWholeTs(nowTS, ident.period)) {
            LOG.info("Try to handle [" + ident.topic + ":" + Util.format.format(new Date(ident.ts)) + "]");
            attemptSource.clear();
            if (!Util.wasDone(ident, ident.ts)) {
                Path[] expectedFile = null;
                for(String kvmSource : ident.kvmSources) {
                    expectedFile = Util.getRollHdfsPath(ident, kvmSource);
                    if (!Util.retryExists(expectedFile)) {
                        LOG.info("None of " + Arrays.toString(expectedFile) + " is ready.");
                        attemptSource.add(kvmSource);
                    }
                }
                for(String paasSource : ident.paasSources) {
                    expectedFile = Util.getRollHdfsPath(ident, paasSource);
                    if (!Util.retryExists(expectedFile)) {
                        LOG.info("None of " + Arrays.toString(expectedFile) + " is ready.");
                        attemptSource.add(paasSource);
                    }
                }
                //skip the source blacklist
                attemptSource.removeAll(lionConfChange.getSkipSourceBlackList());
                
                if (attemptSource.isEmpty()) { //all file ready
                    if (expectedFile != null) {
                        if (!Util.retryTouch(expectedFile[0].getParent(), CheckDone.doneFlag)) {
                            LOG.error("Alarm, failed to touch a done file. " +
                                    "Try in next check cycle. " +
                                    "If you see this message for the second time, " +
                                    "please find out why.");
                            break;
                        } else {
                            LOG.info("[" + ident.topic + ":" + Util.format.format(new Date(ident.ts)) + "]===>Done!");
                        }
                    } else {
                        LOG.fatal("expectedFile is null. It should not be happen.");
                    }
                } else {
                    if (ident.timeout > 0 && ident.timeout < 60 && calendar.get(Calendar.MINUTE) >= ident.timeout) {
                        if (expectedFile != null) {
                            if (!Util.retryTouch(expectedFile[0].getParent(), CheckDone.timeoutFlag)) {
                                LOG.error("Alarm, failed to touch a TIMEOUT_FLAG file. " +
                                        "Try in next check cycle. " +
                                        "If you see this message for the second time, " +
                                        "please find out why.");
                                break;
                            } else {
                                LOG.info("[" + ident.topic + ":" + Util.format.format(new Date(ident.ts)) + "]===>Timeout!");
                            }
                        } else {
                            LOG.fatal("expectedFile is null. It should not be happen.");
                        }
                    } else if (calendar.get(Calendar.MINUTE) >= alartTime) {
                        if (!lionConfChange.getAlarmBlackList().contains(ident.topic)) {
                            LOG.error("Alarm, [" + ident.topic + ":" + Util.format.format(new Date(ident.ts)) + " unfinished, add to TimeChecker.");
                        }
                        timeChecker.registerTimeChecker(ident, ident.ts);
                    } else {
                        break;
                    }
                }
            } else {
                LOG.info("[" + ident.topic + ":" + Util.format.format(new Date(ident.ts)) + "]===>Already Done!");
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
            for (RollIdent ident : rollIdents) {
                CheckDone checker = new CheckDone(ident);
                LOG.info("create a checkdone thread " + checker.toString());
                ScheduledFuture<?> scheduledFuture = checkerThreadPool.scheduleWithFixedDelay(checker, 0, checkperiod, TimeUnit.SECONDS);
                threadMap.put(ident.topic, scheduledFuture);
            }
            LOG.info("Start the Time Checker per " + sleepDuration + "millis.");
            timeChecker.setDaemon(true);
            timeChecker.setName("TimeChecker");
            timeChecker.start();
        } catch (FileNotFoundException e) {
            LOG.error("Oops, got an exception.", e);
        } catch (NumberFormatException e) {
            LOG.error("Oops, got an exception.", e);
        } catch (IOException e) {
            LOG.error("Oops, got an exception.", e);
        } catch (LionException e) {
            LOG.error("Oops, got an exception.", e);
        }
    }

    static void init() throws FileNotFoundException, NumberFormatException, IOException, LionException {
        Properties prop = new Properties();
        Thread currentThread = Thread.currentThread();
        ClassLoader contextClassLoader = currentThread.getContextClassLoader();
        prop.load(contextClassLoader.getResourceAsStream("config.properties"));
        alartTime = Integer.parseInt(prop.getProperty("ALARM_TIME"));
        successprefix = prop.getProperty("SUCCESS_PREFIX", "_SUCCESS.");
        doneFlag = prop.getProperty("DONE_FLAG", "_done");
        timeoutFlag = prop.getProperty("TIMEOUT_FLAG", "_timeout");
        hdfsbasedir = prop.getProperty("HDFS_BASEDIR");
        if (hdfsbasedir.endsWith("/")) {
            hdfsbasedir = hdfsbasedir.substring(0, hdfsbasedir.length() - 1);
        }
        hdfsfilesuffix = prop.getProperty("HDFS_FILE_SUFFIX").split(",");
        checkperiod = Long.parseLong(prop.getProperty("CHECK_PERIOD", "180"));
        fillRollIdent(prop);
        boolean enableSecurity = Boolean.parseBoolean(prop.getProperty("SECURITY.ENABLE", "true"));
        Configuration conf = new Configuration();
        if (enableSecurity) {
            String keytab = prop.getProperty("KEYTAB_FILE");
            String namenodePrincipal = prop.getProperty("NAMENODE.PRINCIPAL");
            String principal = prop.getProperty("PRINCIPAL");
            conf.set("checkdone.keytab", keytab);
            conf.set("dfs.namenode.kerberos.principal", namenodePrincipal);
            conf.set("checkdone.principal", principal);
            conf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(conf);
            SecurityUtil.login(conf, "checkdone.keytab", "checkdone.principal");
        }
        fs = (new Path(hdfsbasedir)).getFileSystem(conf);
        LOG.info("Create thread pool");
        checkerThreadPool = Executors.newScheduledThreadPool(Integer.parseInt(prop.getProperty("MAX_THREAD_NUM", "10")));
        sleepDuration = Long.parseLong(prop.getProperty("TIMECHECKER_PROID", "120000"));
        threadMap = Collections.synchronizedMap(new HashMap<String, ScheduledFuture<?>>());
        timeChecker = new TimeChecker(sleepDuration, lionConfChange);
    }

    private static void fillRollIdent(Properties prop) throws LionException {
        ConfigCache configCache = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
        int apiId = Integer.parseInt(prop.getProperty("LION.ID", "71"));
        lionConfChange = new LionConfChange(configCache, apiId);
        lionConfChange.initLion();
        rollIdents = new ArrayList<RollIdent>();
        for (String appName : lionConfChange.appSet) {
            RollIdent rollIdent = new RollIdent();
            rollIdent.topic = appName;
            List<String> sources = lionConfChange.getAppToHosts().get(appName);
            if (sources == null || sources.isEmpty()) {
                LOG.error("source hosts are all miss for " + appName);
                continue;
            }
            rollIdent.kvmSources = sources;
            Context context = ConfigKeeper.configMap.get(appName);
            if (context == null) {
                LOG.error("Can not get app: " + appName + " from configMap");
                continue;
            }
            rollIdent.period = context.getLong(ParamsKey.Appconf.ROLL_PERIOD);
            rollIdent.cmdbapp = Arrays.asList(context.getString(ParamsKey.Appconf.CMDB_APP)); 
            long rawBeginTs = Long.parseLong(prop.getProperty("BEGIN_TS", String.valueOf(new Date().getTime())));
            rollIdent.ts = Util.getCurrWholeTs(rawBeginTs, rollIdent.period);
            rollIdent.timeout = Integer.parseInt(prop.getProperty(appName + ".TIMEOUT_MINUTE", "-1"));
            rollIdents.add(rollIdent);
        }
        getPaaSInstanceURLPerfix = prop.getProperty("CHECK.PAAS.URL");
        sleepDuration = Long.parseLong(prop.getProperty("PAAS_CHECKER_PROID", "120000"));;
        getInstanceFromPaas = new GetInstanceFromPaas(sleepDuration, rollIdents, getPaaSInstanceURLPerfix);
        getInstanceFromPaas.setName("instanceGetter");
        getInstanceFromPaas.start();
    }

    public static FileSystem fs;
    public static String successprefix;
    public static String hdfsbasedir;
    public static String[] hdfsfilesuffix;
    public static String hdfsHiddenfileprefix = "_";
    private static int alartTime;
    public static long checkperiod;
    public static ScheduledExecutorService checkerThreadPool;
    private static List<RollIdent> rollIdents;
    private static long sleepDuration;
    public static String getPaaSInstanceURLPerfix;
    private static LionConfChange lionConfChange;
    public static Map<String, ScheduledFuture<?>> threadMap;
    public static TimeChecker timeChecker;
    public static GetInstanceFromPaas getInstanceFromPaas;
    public static String doneFlag;
    public static String timeoutFlag;
}

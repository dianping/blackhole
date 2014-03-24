package com.dp.blackhole.appnode;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.cat.Cat;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.gen.AssignCollectorPB.AssignCollector;
import com.dp.blackhole.common.gen.ConfResPB.ConfRes;
import com.dp.blackhole.common.gen.ConfResPB.ConfRes.AppConfRes;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.MessagePB.Message.MessageType;
import com.dp.blackhole.common.gen.RecoveryRollPB.RecoveryRoll;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.conf.Context;
import com.dp.blackhole.exception.BlackholeClientException;
import com.dp.blackhole.node.Node;

public class Appnode extends Node implements Runnable {
    private static final Log LOG = LogFactory.getLog(Appnode.class);
    private ExecutorService pool;
    private ExecutorService recoveryThreadPool;
    private FileListener listener;
    private static Map<String, AppLog> appLogs = new ConcurrentHashMap<String, AppLog>();
    private static Map<AppLog, LogReader> appReaders = new ConcurrentHashMap<AppLog, LogReader>();
    private Map<String, RollRecovery> recoveryingMap = new ConcurrentHashMap<String, RollRecovery>();
    private int confLoopFactor = 1;
    private HeartBeat heartBeat;

    public Appnode() {
        pool = Executors.newCachedThreadPool();
        recoveryThreadPool = Executors.newFixedThreadPool(2);
    }

    @Override
    public boolean process(Message msg) {
        String appName;
        String collectorServer;
        int collectorPort;
        AppLog appLog = null;
        LogReader logReader = null;
        RollRecovery rollRecovery = null;
        MessageType type = msg.getType();
        Random random = new Random();
        switch (type) {
        case NOAVAILABLENODE:
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.info("NOAVAILABLENODE sleep interrupted", e);
                Cat.logError("NOAVAILABLENODE sleep interrupted", e);
            }
            String app = msg.getNoAvailableNode().getAppName();
            AppLog applog = appLogs.get(app);
            register(app, applog.getCreateTime());
            break;
        case RECOVERY_ROLL:
            RecoveryRoll recoveryRoll = msg.getRecoveryRoll();
            appName = recoveryRoll.getAppName();
            if ((appLog = appLogs.get(appName)) != null) {
                long rollTs = recoveryRoll.getRollTs();
                String recoveryKey = appName + ":" + rollTs;
                if ((rollRecovery = recoveryingMap.get(recoveryKey)) == null) {
                    collectorServer = recoveryRoll.getCollectorServer();
                    collectorPort = recoveryRoll.getCollectorPort();
                    rollRecovery = new RollRecovery(this, collectorServer,
                            collectorPort, appLog, rollTs);
                    recoveryingMap.put(recoveryKey, rollRecovery);
                    recoveryThreadPool.execute(rollRecovery);
                    return true;
                } else {
                    LOG.info("duplicated recovery roll message: "
                            + recoveryRoll);
                }
            } else {
                LOG.error("RECOVERY_ROLL: " + recoveryRoll.getAppName()
                        + " from supervisor message not match with local");
                Cat.logError(new BlackholeClientException("RECOVERY_ROLL: " + recoveryRoll.getAppName()
                        + " from supervisor message not match with local"));
            }
            break;
        case ASSIGN_COLLECTOR:
            AssignCollector assignCollector = msg.getAssignCollector();
            appName = assignCollector.getAppName();
            if ((appLog = appLogs.get(appName)) != null) {
                if ((logReader = appReaders.get(appLog)) == null) {
                    collectorServer = assignCollector.getCollectorServer();
                    collectorPort = assignCollector.getCollectorPort();
                    logReader = new LogReader(this, collectorServer, collectorPort, appLog);
                    appReaders.put(appLog, logReader);
                    pool.execute(logReader);
                    return true;
                } else {
                    LOG.info("duplicated assign collector message: " + assignCollector);
                }
            } else {
                LOG.error("ASSIGN_COLLECTOR: " + assignCollector.getAppName()
                        + " from supervisor message not match with local");
                Cat.logError(new BlackholeClientException("ASSIGN_COLLECTOR: " + assignCollector.getAppName()
                        + " from supervisor message not match with local"));
            }
            break;
        case NOAVAILABLECONF:
            if (confLoopFactor < 30) {
                confLoopFactor = confLoopFactor << 1;
            }
            int randomSecond = confLoopFactor * (random.nextInt(21) + 40) * 1000;
            LOG.info("Configurations not ready, sleep " + randomSecond/1000 + " second.");
            try {
                Thread.sleep(randomSecond);
            } catch (InterruptedException e) {
                LOG.info("NOAVAILABLECONF sleep interrupted", e);
                Cat.logError("NOAVAILABLECONF sleep interrupted", e);
            }
            requireConfigFromSupersivor();
            break;
        case CONF_RES:
            confLoopFactor = 1;
            ConfigKeeper confKeeper = new ConfigKeeper();
            ConfRes confRes = msg.getConfRes();
            List<AppConfRes> appConfResList = confRes.getAppConfResList();
            for (AppConfRes appConfRes : appConfResList) {
                confKeeper.addRawProperty(appConfRes.getAppName() + "."
                        + ParamsKey.Appconf.WATCH_FILE, appConfRes.getWatchFile());
                confKeeper.addRawProperty(appConfRes.getAppName() + "."
                        + ParamsKey.Appconf.ROLL_PERIOD, appConfRes.getPeriod());
                confKeeper.addRawProperty(appConfRes.getAppName() + "."
                        + ParamsKey.Appconf.MAX_LINE_SIZE, appConfRes.getMaxLineSize());
            }
            if (!checkAllFilesExist()) {
                LOG.error("Configurations are incorrect, sleep 60 seconds..");
                Cat.logError(new BlackholeClientException("Configurations are incorrect, sleep 60 seconds.."));
                try {
                    Thread.sleep(60 * 1000);
                } catch (InterruptedException e) {
                    LOG.error("Oops, sleep interrupted", e);
                    Cat.logError("Oops, sleep interrupted", e);
                }
                requireConfigFromSupersivor();
                break;
            }
            fillUpAppLogsFromConfig();
            registerApps();
            if (this.heartBeat == null || !this.heartBeat.isAlive()) {
                this.heartBeat = new HeartBeat();
                this.heartBeat.setDaemon(true);
                this.heartBeat.start();
            }
            break;
        default:
            LOG.error("Illegal message type " + msg.getType());
            Cat.logError(new BlackholeClientException("Illegal message type " + msg.getType()));
        }
        return false;
    }

    private void register(String appName, long regTimestamp) {
        Message msg = PBwrap.wrapAppReg(appName, getHost(), regTimestamp);
        super.send(msg);
    }

    public boolean checkAllFilesExist() {
        boolean res = true;
        try {
            String hostname = Util.getLocalHost();
            for (Map.Entry<String, Context> entry : ConfigKeeper.configMap.entrySet()) {
                String appName = entry.getKey();
                Context context = entry.getValue();
                String pathCandidateStr = context.getString(ParamsKey.Appconf.WATCH_FILE);
                if (pathCandidateStr == null) {
                    LOG.error("Oops, can not get WATCH_FILE from mapping for app " + appName);
                    Cat.logError(new BlackholeClientException("Oops, can not get WATCH_FILE from mapping for app " + appName));
                    return false;
                }
                String[] pathCandidates = pathCandidateStr.split("\\s+");
                for (int i = 0; i < pathCandidates.length; i++) {
                    File fileForTest = new File(pathCandidates[i]);
                    if (fileForTest.exists()) {
                        LOG.info("Check file " + pathCandidates[i] + " ok.");
                        context.put(ParamsKey.Appconf.WATCH_FILE, pathCandidates[i]);
                        break;
                    } else if (isCompatibleWithOldVersion(context, fileForTest, hostname)) {
                        LOG.info("It's an old version of log printer. Ok");
                        break;
                    } else {
                        if (i == pathCandidates.length - 1) {
                            LOG.error("Appnode process start faild, because all of file " + Arrays.toString(pathCandidates) + " not found!");
                            Cat.logError(new BlackholeClientException("Appnode process start faild, because all of file "
                                            + pathCandidates + " not found!"));
                            res = false;
                        }
                    }
                }
            }
        } catch (UnknownHostException e) {
            LOG.error("Oops, unknow host, maybe cause by DNS exception.", e);
            Cat.logError("Oops, unknow host, maybe cause by DNS exception.", e);
            res = false;
        }
        return res;
    }

    private boolean isCompatibleWithOldVersion(Context context, final File fileForTest, String hostname) {
        if (fileForTest.getParent() == null || !fileForTest.getParentFile().exists()) {
            return false;
        }
        int index = fileForTest.getName().indexOf('.');
        if (index == -1) {
            LOG.error("Invaild fileName " + fileForTest.getName());
            Cat.logError(new BlackholeClientException("Invaild fileName " + fileForTest.getName()));
            return false;
        }
        String specifiedName = fileForTest.getName().substring(0, index);
        if (specifiedName == null) {
            return false;
        }
        if (hostname.contains(specifiedName)) {
            String oldVersionPath = fileForTest.getParent() + "/" + hostname + fileForTest.getName().substring(index);
            if (new File(oldVersionPath).exists()) {
                context.put(ParamsKey.Appconf.WATCH_FILE, oldVersionPath);
                LOG.debug("WATCH_FILE change to " + oldVersionPath);
                return true;
            }
        }
        return false;
    }

    @Override
    public void run() {
        // hard code, please modify to real supervisor address before mvn package
        Properties prop = new Properties();
        try {
          prop.load(ClassLoader.getSystemResourceAsStream("META-INF/app.properties"));
        } catch (IOException e) {
          LOG.fatal("Load app.properties file fail.", e);
          Cat.logError("Load app.properties file fail.", e);
          return;
        }
        String supervisorHost = prop.getProperty("supervisor.host");
        if (supervisorHost == null) {
          LOG.fatal("Oops, no supervisor.host provided. Thread quit.");
          Cat.logError(new BlackholeClientException("Oops, no supervisor.host provided."));
          return;
        }
        String supervisorPort = prop.getProperty("supervisor.port", "6999");
        try {
            init(supervisorHost, Integer.parseInt(supervisorPort));
        } catch (NumberFormatException e) {
            LOG.error(e.getMessage(), e);
            Cat.logError(e);
            return;
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage(), e);
            Cat.logError(e);
            return;
        }
        try {
            listener = new FileListener();
        } catch (Exception e) {
            LOG.error("Failed to create a file listener, node shutdown!", e);
            Cat.logError("Failed to create a file listener, node shutdown!", e);
            return;
        }
        // wait for receiving message from supervisor
        super.loop();
    }

    public void fillUpAppLogsFromConfig() {
        for (Map.Entry<String, Context> entry : ConfigKeeper.configMap.entrySet()) {
            String appName = entry.getKey();
            Context context = entry.getValue();
            String path = context.getString(ParamsKey.Appconf.WATCH_FILE);
            long rollPeroid = context.getLong(ParamsKey.Appconf.ROLL_PERIOD);
            int maxLineSize = context.getInteger(ParamsKey.Appconf.MAX_LINE_SIZE, 512000);
            AppLog appLog = new AppLog(appName, path, rollPeroid, maxLineSize);
            appLogs.put(appName, appLog);
        }
    }

    public void shutdown() {
        super.shutdown();
    }

    @Override
    protected void onDisconnected() {
        confLoopFactor = 1;
        // close connected streams
        LOG.info("shutdown app node");
        for (Map.Entry<AppLog, LogReader> e : appReaders.entrySet()) {
            LogReader reader = e.getValue();
            reader.stop();
            appReaders.remove(e.getKey());
        }
        for (Map.Entry<String, RollRecovery> e : recoveryingMap.entrySet()) {
            RollRecovery recovery = e.getValue();
            recovery.stop();
            recoveryingMap.remove(e.getKey());
        }
        appLogs.clear();
        ConfigKeeper.configMap.clear();
        if (heartBeat != null) {
            heartBeat.shutdown();
            heartBeat = null;
        }
    }

    @Override
    protected void onConnected() {
        clearMessageQueue();
        requireConfigFromSupersivor();
    }

    private void requireConfigFromSupersivor() {
        Message msg = PBwrap.wrapConfReq();
        super.send(msg);
    }

    private void registerApps() {
        // register the app to supervisor
        for (AppLog appLog : appLogs.values()) {
            register(appLog.getAppName(), Util.getTS());
        }
    }

    public FileListener getListener() {
        return listener;
    }

    /**
     * Just for unit test
     **/
    public void setListener(FileListener listener) {
        this.listener = listener;
    }

    public void reportFailure(String app, String appHost, final long ts) {
        Message message = PBwrap.wrapAppFailure(app, appHost, ts);
        send(message);
        AppLog applog = appLogs.get(app);
        appReaders.remove(applog);
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            LOG.info("report failure sleep interrupted", e);
            Cat.logError("report failure sleep interrupted", e);
        }
        register(app, applog.getCreateTime());
    }

    public void reportUnrecoverable(String appname, String appHost, final long period, final long rollTs) {
        Message message = PBwrap.wrapUnrecoverable(appname, appHost, period, rollTs);
        send(message);
    }

    public void reportRecoveryFail(String appname, String appServer, final long rollTs) {
        Message message = PBwrap.wrapRecoveryFail(appname, appServer, rollTs);
        send(message);
    }

    public void removeRecoverying(String appname, final long rollTs) {
        String recoveryKey = appname + ":" + rollTs;
        recoveryingMap.remove(recoveryKey);
    }

    // for test
    public static void main(String[] args) {
        Appnode appnode = new Appnode();
        Thread thread = new Thread(appnode);
        thread.start();
    }
}

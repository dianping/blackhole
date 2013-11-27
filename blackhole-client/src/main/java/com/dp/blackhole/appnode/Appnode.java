package com.dp.blackhole.appnode;

import java.io.File;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import com.dp.blackhole.node.Node;

public class Appnode extends Node implements Runnable {
    private static final Log LOG = LogFactory.getLog(Appnode.class);
    private ExecutorService pool;
    private ExecutorService recoveryThreadPool;
    private FileListener listener;
    private static Map<String, AppLog> appLogs = new ConcurrentHashMap<String, AppLog>();
    private static Map<AppLog, LogReader> appReaders = new ConcurrentHashMap<AppLog, LogReader>();
    
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
  	    MessageType type = msg.getType();
  	    switch (type) {
  	    case NOAVAILABLENODE:
  	        try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                LOG.info("NOAVAILABLENODE sleep interrupted");
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
                collectorServer = recoveryRoll.getCollectorServer();
                collectorPort =  recoveryRoll.getCollectorPort();
                RollRecovery recovery = new RollRecovery(this, collectorServer, collectorPort, appLog, rollTs);
                recoveryThreadPool.execute(recovery);
                return true;
            } else {
                LOG.error("AppName [" + recoveryRoll.getAppName()
                        + "] from supervisor message not match with local");
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
                LOG.error("AppName [" + assignCollector.getAppName()
                        + "] from supervisor message not match with local");
            }
            break;
        case NOAVAILABLECONF:
            LOG.info("Configurations not ready, sleep 5 seconds..");
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                LOG.info("NOAVAILABLECONF sleep interrupted");
            }
            requireConfigFromSupersivor();
            break;
        case CONF_RES:
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
                Thread.currentThread().interrupt();
            }
            fillUpAppLogsFromConfig();
            registerApps();
            break;
        default:
            LOG.error("Illegal message type " + msg.getType());
        }
  	    return false;
    }

    private void register(String appName, long regTimestamp) {
        Message msg = PBwrap.wrapAppReg(appName, getHost(), regTimestamp);
        super.send(msg);
    }

    private boolean checkAllFilesExist() {
        boolean res = true;
        for (String appName : ConfigKeeper.configMap.keySet()) {
            String path = ConfigKeeper.configMap.get(appName)
                    .getString(ParamsKey.Appconf.WATCH_FILE);
            File fileForTest = new File(path);
            if (!fileForTest.exists()) {
                LOG.error("Appnode process start faild, because file " + path + " not found. Thread interrupt!");
                res = false;
            } else {
                LOG.info("Check file " + path + " ok.");
            }
        }
        return res;
    }

    @Override
    public void run() {
        //hard code, please modify to real supervisor address before mvn package
        String supervisorHost = "test90.hadoop";
        String supervisorPort = "6999";
        try {
            init(supervisorHost, Integer.parseInt(supervisorPort));
        } catch (NumberFormatException e) {
            LOG.error(e.getMessage(), e);
            return;
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage(), e);
            return;
        }
        try {    
            listener = new FileListener();
        } catch (Exception e) {
            LOG.error("Failed to create a file listener, node shutdown!", e);
            return;
        }
        //wait for receiving message from supervisor
        super.loop();
    }

    public void fillUpAppLogsFromConfig() {
        for (String appName : ConfigKeeper.configMap.keySet()) {
            String path = ConfigKeeper.configMap.get(appName)
                    .getString(ParamsKey.Appconf.WATCH_FILE);
            int maxLineSize = ConfigKeeper.configMap.get(appName)
                    .getInteger(ParamsKey.Appconf.MAX_LINE_SIZE, 65536);
            AppLog appLog = new AppLog(appName, path, maxLineSize);
            appLogs.put(appName, appLog);
        }
    }

    public void shutdown() {
        super.shutdown();
    }

    @Override
    protected void onDisconnected() {
        // close connected streams
        LOG.info("shutdown app node");
        for (java.util.Map.Entry<AppLog, LogReader> e : appReaders.entrySet()) {
            LogReader reader = e.getValue();
            reader.stop();
            appReaders.remove(e.getKey());
        }
        appLogs.clear();
        ConfigKeeper.configMap.clear();
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
        //register the app to supervisor
        for (AppLog appLog : appLogs.values()) {
//            register(appLog.getAppName(), appLog.getCreateTime());
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

    public void reportFailure(String app, String appHost, long ts) {
        Message message = PBwrap.wrapAppFailure(app, appHost, ts);
        send(message);
        AppLog applog = appLogs.get(app);
        appReaders.remove(applog);
        register(app, applog.getCreateTime());
    }
    
    public void reportUnrecoverable(String appName, String appHost, long ts) {
        Message message = PBwrap.wrapUnrecoverable(appName, appHost, ts);
        send(message);
    }
    
    //for test
    public static void main(String[] args) {
        Appnode appnode = new Appnode();
        Thread thread = new Thread(appnode);
        thread.start();
    }
}

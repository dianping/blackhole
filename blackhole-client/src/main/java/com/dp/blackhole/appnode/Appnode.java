package com.dp.blackhole.appnode;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import com.dp.blackhole.exception.BlackholeClientException;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.HeartBeat;
import com.dp.blackhole.network.SimpleConnection;
import com.google.protobuf.InvalidProtocolBufferException;

public class Appnode implements Runnable {
    private static final Log LOG = LogFactory.getLog(Appnode.class);
    private ExecutorService pool;
    private ExecutorService recoveryThreadPool;
    private FileListener listener;
    protected int recoveryPort;
    private int brokerPort;
    private String hostname;
    private static Map<String, AppLog> appLogs = new ConcurrentHashMap<String, AppLog>();
    private static Map<AppLog, LogReader> appReaders = new ConcurrentHashMap<AppLog, LogReader>();
    private Map<String, RollRecovery> recoveryingMap = new ConcurrentHashMap<String, RollRecovery>();
    private ThroughputStat stat;
    
    private GenClient<ByteBuffer, SimpleConnection, AgentProcessor> client;
    AgentProcessor processor;
    private SimpleConnection supervisor;
    
    public Appnode() {
        pool = Executors.newCachedThreadPool();
        recoveryThreadPool = Executors.newFixedThreadPool(2);
    }

    public String getHost() {
        return hostname;
    }
    
    private void register(String appName, long regTimestamp) {
        Message msg = PBwrap.wrapAppReg(appName, hostname, regTimestamp);
        send(msg);
    }

    public boolean checkAllFilesExist() {
        boolean res = true;
        for (String appName : ConfigKeeper.configMap.keySet()) {
            String pathCandidateStr = ConfigKeeper.configMap.get(appName).getString(
                    ParamsKey.Appconf.WATCH_FILE);
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
                    ConfigKeeper.configMap.get(appName).put(ParamsKey.Appconf.WATCH_FILE, pathCandidates[i]);
                    break;
                } else if (isCompatibleWithOldVersion(appName, fileForTest, hostname)) {
                    LOG.info("It's an old version of log printer. Ok");
                    break;
                } else {
                    if (i == pathCandidates.length - 1) {
                        LOG.error("Appnode process start faild, because all of file " + pathCandidates + " not found!");
                        Cat.logError(new BlackholeClientException("Appnode process start faild, because all of file "
                                        + pathCandidates + " not found!"));
                        res = false;
                    }
                }
            }
        }
        return res;
    }
    
    private boolean isCompatibleWithOldVersion(String appName, final File fileForTest, String hostname) {
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
                ConfigKeeper.configMap.get(appName).put(ParamsKey.Appconf.WATCH_FILE, oldVersionPath);
                LOG.debug("WATCH_FILE change to " + oldVersionPath);
                return true;
            }
        }
        return false;
    }

    @Override
    public void run() {
        try {
            hostname = Util.getLocalHost();
        } catch (UnknownHostException e) {
            LOG.error("can not get localhost: ", e);
            Cat.logError("can not get localhost: ", e);
            return;
        }
        
        //  hard code, please modify to real supervisor address before mvn package
        Properties prop = new Properties();
        try {
            prop.load(ClassLoader.getSystemResourceAsStream("META-INF/app.properties"));
        } catch (IOException e) {
            LOG.fatal("Load app.properties file fail.", e);
            Cat.logError("Load app.properties file fail.", e);
            return;
        }    
        recoveryPort = Integer.parseInt(prop.getProperty("broker.recovery.port"));      
        brokerPort = Integer.parseInt(prop.getProperty("broker.service.port"));

        try {    
            listener = new FileListener();
        } catch (Exception e) {
            LOG.error("Failed to create a file listener, agent shutdown!", e);
            Cat.logError("Failed to create a file listener, agent shutdown!", e);
            return;
        }
        
        long statPeriodMillis = Long.parseLong(prop.getProperty("stat.thoughput.periodMillis", "60000"));
        initThroughputStat(statPeriodMillis);
        this.stat.start();
        
        processor = new AgentProcessor();
        client = new GenClient(
                processor,
                new SimpleConnection.SimpleConnectionFactory(),
                null);

        try {
            client.init(prop, "agent", "supervisor.host", "supervisor.port");
        } catch (ClosedChannelException e) {
            LOG.error(e.getMessage(), e);
            Cat.logError(e);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            Cat.logError(e);
        }
    }

    public void initThroughputStat(long statPeriodMillis) {
        this.stat = new ThroughputStat(statPeriodMillis);
    }
    
    public void fillUpAppLogsFromConfig() {
        for (String appName : ConfigKeeper.configMap.keySet()) {
            String path = ConfigKeeper.configMap.get(appName)
                    .getString(ParamsKey.Appconf.WATCH_FILE);
            long rollPeroid = ConfigKeeper.configMap.get(appName)
                            .getLong(ParamsKey.Appconf.ROLL_PERIOD);
            int maxLineSize = ConfigKeeper.configMap.get(appName).getInteger(
                    ParamsKey.Appconf.MAX_LINE_SIZE, 65536);
            AppLog appLog = new AppLog(appName, path, rollPeroid, maxLineSize);
            appLogs.put(appName, appLog);
        }
    }

    private void requireConfigFromSupersivor() {
        Message msg = PBwrap.wrapConfReq();
        send(msg);
    }
    
    private void registerApps() {
        //register the app to supervisor
        for (AppLog appLog : appLogs.values()) {
            register(appLog.getAppName(), Util.getTS());
        }
    }
    
    public FileListener getListener() {
        return listener;
    }

    public void shutdown() {
        pool.shutdownNow();
        recoveryThreadPool.shutdownNow();
        client.shutdown();
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
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            LOG.info("report failure sleep interrupted", e);
            Cat.logError("report failure sleep interrupted", e);
        }
        register(app, applog.getCreateTime());
    }
    
    public void reportUnrecoverable(String appName, String appHost, long ts) {
        Message message = PBwrap.wrapUnrecoverable(appName, appHost, ts);
        send(message);
    }
    
    public void reportRecoveryFail(final String appname, final String appServer, final long rollTs) {
        Message message = PBwrap.wrapRecoveryFail(appname, appServer, rollTs);
        send(message);
    }
    
    public void removeRecoverying(final String appname, final long rollTs) {
        String recoveryKey = appname + ":" + rollTs;
        recoveryingMap.remove(recoveryKey);
    }
    
    public void send(Message message) {
        LOG.debug("send: " + message);
        if (supervisor != null) {
            supervisor.send(PBwrap.PB2Buf(message));
        }
    }
    
    public class AgentProcessor implements EntityProcessor<ByteBuffer, SimpleConnection> {
        private HeartBeat heartbeat = null;
    	
        @Override
        public void OnConnected(SimpleConnection connection) {
            supervisor = connection;
            requireConfigFromSupersivor();
            heartbeat = new HeartBeat(supervisor);
            heartbeat.start();
        }

        @Override
        public void OnDisconnected(SimpleConnection connection) {
            supervisor.close();
            supervisor = null;
            
            // close connected streams
            LOG.info("shutdown app node");
            for (java.util.Map.Entry<AppLog, LogReader> e : appReaders.entrySet()) {
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
            
            heartbeat.shutdown();
            heartbeat = null;
        }
        
        @Override
        public void process(ByteBuffer reply, SimpleConnection from) {

            Message msg = null;
            try {
                msg = PBwrap.Buf2PB(reply);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("InvalidProtocolBufferException catched: ", e);
                return;
            }

            LOG.debug("agent received: " + msg);         
            processInternal(msg);
        }

        boolean processInternal(Message msg) {
			String appName;
            String broker;
            AppLog appLog = null;
            LogReader logReader = null;
            RollRecovery rollRecovery = null;
            
            MessageType type = msg.getType();
            switch (type) {
            case NOAVAILABLENODE:
                try {
                    Thread.sleep(5 * 1000);
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
                        broker = recoveryRoll.getCollectorServer();
                        //TODO broker recovery port ?
                        rollRecovery = new RollRecovery(Appnode.this,
                                broker, recoveryPort, appLog, rollTs);
                        recoveryingMap.put(recoveryKey, rollRecovery);
                        recoveryThreadPool.execute(rollRecovery);
                        return true;
                    } else {
                        LOG.info("duplicated recovery roll message: "
                                + recoveryRoll);
                    }
                } else {
                    LOG.error("AppName [" + recoveryRoll.getAppName()
                            + "] from supervisor message not match with local");
                    Cat.logError(new BlackholeClientException("RECOVERY_ROLL: " + recoveryRoll.getAppName()
                            + " from supervisor message not match with local"));
                }
                break;
            case ASSIGN_COLLECTOR:
                AssignCollector assignCollector = msg.getAssignCollector();
                appName = assignCollector.getAppName();
                if ((appLog = appLogs.get(appName)) != null) {
                    if ((logReader = appReaders.get(appLog)) == null) {
                        broker = assignCollector.getCollectorServer();
                        // TODO brokerPort?
                        logReader = new LogReader(Appnode.this, hostname, broker, brokerPort,
                                appLog);
                        appReaders.put(appLog, logReader);
                        pool.execute(logReader);
                        stat.add(logReader);
                        return true;
                    } else {
                        LOG.info("duplicated assign collector message: "
                                + assignCollector);
                    }
                } else {
                    LOG.error("AppName [" + assignCollector.getAppName()
                            + "] from supervisor message not match with local");
                    Cat.logError(new BlackholeClientException("ASSIGN_COLLECTOR: " + assignCollector.getAppName()
                            + " from supervisor message not match with local"));
                }
                break;
            case NOAVAILABLECONF:
                LOG.info("Configurations not ready, sleep 5 seconds..");
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    LOG.info("NOAVAILABLECONF sleep interrupted", e);
                    Cat.logError("NOAVAILABLECONF sleep interrupted", e);
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
                    LOG.error("Configurations are incorrect, sleep 5 seconds..");
                    Cat.logError(new BlackholeClientException("Configurations are incorrect, sleep 5 seconds.."));
                    try {
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e) {
                        LOG.error("Oops, sleep interrupted", e);
                        Cat.logError("Oops, sleep interrupted", e);
                    }
                    requireConfigFromSupersivor();
                    break;
                }
                fillUpAppLogsFromConfig();
                registerApps();
                break;
            default:
                LOG.error("Illegal message type " + msg.getType());
                Cat.logError(new BlackholeClientException("Illegal message type " + msg.getType()));
            }
            return false;
		}
    }
    
    public static void main(String[] args) {
        Appnode appnode = new Appnode();
        Thread thread = new Thread(appnode);
        thread.start();
    }
}

package com.dp.blackhole.agent;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.cat.Cat;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.exception.BlackholeClientException;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.HeartBeat;
import com.dp.blackhole.network.SimpleConnection;
import com.dp.blackhole.protocol.control.AssignCollectorPB.AssignCollector;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes.AppConfRes;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.MessagePB.Message.MessageType;
import com.dp.blackhole.protocol.control.RecoveryRollPB.RecoveryRoll;
import com.google.protobuf.InvalidProtocolBufferException;

public class Agent implements Runnable {
    private static final Log LOG = LogFactory.getLog(Agent.class);
    private static final int DEFAULT_DELAY_SECOND = 5;
    private ExecutorService pool;
    private ExecutorService recoveryThreadPool;
    private FileListener listener;
    private String hostname;
    private final ScheduledThreadPoolExecutor scheduler;
    private static Map<String, AppLog> appLogs = new ConcurrentHashMap<String, AppLog>();
    private static Map<AppLog, LogReader> appReaders = new ConcurrentHashMap<AppLog, LogReader>();
    private Map<String, RollRecovery> recoveryingMap = new ConcurrentHashMap<String, RollRecovery>();
    
    private GenClient<ByteBuffer, SimpleConnection, AgentProcessor> client;
    AgentProcessor processor;
    private SimpleConnection supervisor;
    private ConfigKeeper confKeeper;
    private int confLoopFactor = 1;

    public Agent() {
        pool = Executors.newCachedThreadPool();
        recoveryThreadPool = Executors.newFixedThreadPool(2);
        confKeeper = new ConfigKeeper();
        scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    public String getHost() {
        return hostname;
    }
    
    private void register(String appName, long regTimestamp) {
        Message msg = PBwrap.wrapAppReg(appName, hostname, regTimestamp);
        send(msg, DEFAULT_DELAY_SECOND);
    }

    public boolean checkFilesExist(String appName, String pathCandidateStr) {
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
                confKeeper.addRawProperty(appName + "."
                        + ParamsKey.Appconf.WATCH_FILE, pathCandidates[i]);
                break;
            } else if (isCompatibleWithOldVersion(appName, fileForTest, getHost(), confKeeper)) {
                LOG.info("It's an old version of log printer. Ok");
                break;
            } else {
                if (i == pathCandidates.length - 1) {
                    LOG.error("App: " + appName + ", Log: " + Arrays.toString(pathCandidates) + " not found!");
                    Cat.logError(new BlackholeClientException("App: " + appName + ", Log: " + Arrays.toString(pathCandidates) + " not found!"));
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isCompatibleWithOldVersion(String appName, final File fileForTest, String hostname, ConfigKeeper confKeeper) {
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
                confKeeper.addRawProperty(appName + "."
                        + ParamsKey.Appconf.WATCH_FILE, oldVersionPath);
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

        try {    
            listener = new FileListener();
        } catch (Exception e) {
            LOG.error("Failed to create a file listener, agent shutdown!", e);
            Cat.logError("Failed to create a file listener, agent shutdown!", e);
            return;
        }
        
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
    
    public void fillUpAppLogsFromConfig(String appName) {
        String path = ConfigKeeper.configMap.get(appName).getString(ParamsKey.Appconf.WATCH_FILE);
        long rollPeroid = ConfigKeeper.configMap.get(appName).getLong(ParamsKey.Appconf.ROLL_PERIOD);
        int maxLineSize = ConfigKeeper.configMap.get(appName).getInteger(ParamsKey.Appconf.MAX_LINE_SIZE, 512000);
        AppLog appLog = new AppLog(appName, path, rollPeroid, maxLineSize);
        appLogs.put(appName, appLog);
    }

    private void requireConfigFromSupersivor(int delaySecond) {
        Message msg = PBwrap.wrapConfReq();
        send(msg, delaySecond);
    }
    
    public FileListener getListener() {
        return listener;
    }

    public void shutdown() {
        pool.shutdownNow();
        recoveryThreadPool.shutdownNow();
        client.shutdown();
        scheduler.shutdown();
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
    
    public void send(Message message) {
        LOG.debug("send: " + message);
        if (supervisor != null) {
            supervisor.send(PBwrap.PB2Buf(message));
        }
    }
    
    public void send(Message message, int delaySecond) {
        scheduler.schedule(new SendTask(message), delaySecond, TimeUnit.SECONDS);
    }

    class SendTask implements Runnable {
        private Message msg;

        public SendTask(Message msg) {
            this.msg = msg;
        }

        @Override
        public void run() {
            send(msg);
        }
    }

    public class AgentProcessor implements EntityProcessor<ByteBuffer, SimpleConnection> {
        private HeartBeat heartbeat = null;

        
        @Override
        public void OnConnected(SimpleConnection connection) {
            supervisor = connection;
            requireConfigFromSupersivor(0);
        }

        @Override
        public void OnDisconnected(SimpleConnection connection) {
            confLoopFactor = 1;
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
            
            if (heartbeat != null) {
                heartbeat.shutdown();
                heartbeat = null;
            }
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
            Random random = new Random();
            switch (type) {
            case NOAVAILABLENODE:
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
                        int recoveryPort = recoveryRoll.getRecoveryPort();
                        rollRecovery = new RollRecovery(Agent.this,
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
                        int brokerPort = assignCollector.getBrokerPort();
                        logReader = new LogReader(Agent.this, hostname, broker, brokerPort,
                                appLog);
                        appReaders.put(appLog, logReader);
                        pool.execute(logReader);
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
                if (confLoopFactor < 30) {
                    confLoopFactor = confLoopFactor << 1;
                }
                int randomSecond = confLoopFactor * (random.nextInt(21) + 40);
                LOG.info("Configurations not ready, sleep " + randomSecond + " second.");
                requireConfigFromSupersivor(randomSecond);
                break;
            case CONF_RES:
                confLoopFactor = 1;
                ConfRes confRes = msg.getConfRes();
                List<AppConfRes> appConfResList = confRes.getAppConfResList();
                for (AppConfRes appConfRes : appConfResList) {
                    appName = appConfRes.getAppName();
                    if (appLogs.containsKey(appName)) {
                        continue;
                    }
                    String pathCandidateStr = appConfRes.getWatchFile();
                    //check files existence
                    if (!checkFilesExist(appName, pathCandidateStr)) {
                        continue;
                    }
                    confKeeper.addRawProperty(appName + "."
                            + ParamsKey.Appconf.ROLL_PERIOD, appConfRes.getPeriod());
                    confKeeper.addRawProperty(appName + "."
                            + ParamsKey.Appconf.MAX_LINE_SIZE, appConfRes.getMaxLineSize());
                    
                    fillUpAppLogsFromConfig(appName);
                    register(appName, Util.getTS());
                    if (this.heartbeat == null || !this.heartbeat.isAlive()) {
                        this.heartbeat = new HeartBeat(supervisor);
                        this.heartbeat.setDaemon(true);
                        this.heartbeat.start();
                    }
                }
                if (appLogs.size() < appConfResList.size()) {
                    LOG.warn("Not all configurations are correct, sleep 5 minutes...");
                    requireConfigFromSupersivor(5 * 60);
                }
                break;
            default:
                LOG.error("Illegal message type " + msg.getType());
                Cat.logError(new BlackholeClientException("Illegal message type " + msg.getType()));
            }
            return false;
        }
    }
    
    public static void main(String[] args) {
        Agent agent = new Agent();
        Thread thread = new Thread(agent);
        thread.start();
    }
}

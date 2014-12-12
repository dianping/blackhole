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

import com.dp.blackhole.agent.TopicMeta.TopicId;
import com.dp.blackhole.agent.persist.IState;
import com.dp.blackhole.common.DaemonThreadFactory;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.HeartBeat;
import com.dp.blackhole.network.SimpleConnection;
import com.dp.blackhole.protocol.control.AssignBrokerPB.AssignBroker;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes.AppConfRes;
import com.dp.blackhole.protocol.control.ConfResPB.ConfRes.LxcConfRes;
import com.dp.blackhole.protocol.control.MessagePB.Message;
import com.dp.blackhole.protocol.control.MessagePB.Message.MessageType;
import com.dp.blackhole.protocol.control.QuitAndCleanPB.Clean;
import com.dp.blackhole.protocol.control.QuitAndCleanPB.InstanceGroup;
import com.dp.blackhole.protocol.control.QuitAndCleanPB.Quit;
import com.dp.blackhole.protocol.control.RecoveryRollPB.RecoveryRoll;
import com.google.protobuf.InvalidProtocolBufferException;

public class Agent implements Runnable {
    private static final Log LOG = LogFactory.getLog(Agent.class);
    private static final int DEFAULT_DELAY_SECOND = 5;
    private final ConfigKeeper confKeeper = new ConfigKeeper();
    private ExecutorService pool;
    private ExecutorService recoveryThreadPool;
    private FileListener listener;
    private String hostname;
    private ScheduledThreadPoolExecutor scheduler;
    private static Map<TopicId, TopicMeta> logMetas = new ConcurrentHashMap<TopicId, TopicMeta>();
    private static Map<TopicMeta, LogReader> topicReaders = new ConcurrentHashMap<TopicMeta, LogReader>();
    private Map<String, RollRecovery> recoveryingMap = new ConcurrentHashMap<String, RollRecovery>();
    
    private GenClient<ByteBuffer, SimpleConnection, AgentProcessor> client;
    AgentProcessor processor;
    private SimpleConnection supervisor;
    private int confLoopFactor = 1;
    private final String baseDirWildcard;
    private boolean paasModel = false;
    private String snapshotPersistDir;
    
    public Agent() {
        this(null);
    }
    
    public Agent(String baseDirWildcard) {
        this.baseDirWildcard = baseDirWildcard;
        if (baseDirWildcard != null) {
            paasModel = true;
            LOG.info("Agent deployed for LXC.");
        } else {
            LOG.info("Agent deployed for KVM.");
        }
        pool = Executors.newCachedThreadPool(new DaemonThreadFactory("LogReader"));
        recoveryThreadPool = Executors.newFixedThreadPool(2, new DaemonThreadFactory("Recovery"));
        scheduler = new ScheduledThreadPoolExecutor(1, new DaemonThreadFactory("Scheduler"));
        
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    public String getHost() {
        return hostname;
    }
    
    public String getBaseDirWildcard() {
        return baseDirWildcard;
    }

    public boolean isPaasModel() {
        return paasModel;
    }
    
    public static Map<TopicMeta, LogReader> getTopicReaders() {
        return topicReaders;
    }

    private void register(TopicId topicId, long regTimestamp) {
        Message msg = PBwrap.wrapTopicReg(topicId.getTopic(),
                Util.getSource(hostname, topicId.getInstanceId()), regTimestamp);
        send(msg, DEFAULT_DELAY_SECOND);
    }

    public boolean checkFilesExist(String topic, String pathCandidateStr) {
        if (pathCandidateStr == null) {
            LOG.error("Oops, can not get WATCH_FILE from mapping for topic " + topic);
            return false;
        }
        String[] pathCandidates = pathCandidateStr.split("\\s+");
        for (int i = 0; i < pathCandidates.length; i++) {
            File fileForTest = new File(pathCandidates[i]);
            if (fileForTest.exists()) {
                LOG.info("Check file " + pathCandidates[i] + " ok.");
                confKeeper.addRawProperty(topic + "."
                        + ParamsKey.TopicConf.WATCH_FILE, pathCandidates[i]);
                break;
            } else {
                if (i == pathCandidates.length - 1) {
                    LOG.error("Topic: " + topic + ", Log: " + Arrays.toString(pathCandidates) + " not found!");
                    return false;
                }
            }
        }
        return true;
    }
    
    public boolean checkFilesExist(String topic, String watchFile, String instanceId) {
        if (watchFile == null || watchFile.trim().length() == 0) {
            return false;
        }
        String realWatchFile = String.format(baseDirWildcard, instanceId) + watchFile;
        File fileForTest = new File(realWatchFile);
        if (fileForTest.exists()) {
            LOG.info("Check file " + realWatchFile + " ok.");
            confKeeper.addRawProperty(topic + "."
                    + ParamsKey.TopicConf.WATCH_FILE, realWatchFile);
            return true;
        } else {
            LOG.error("Topic: " + topic + ", Log: " + realWatchFile + " not found!");
            return false;
        }
    }

    @Override
    public void run() {
        try {
            hostname = Util.getLocalHost();
        } catch (UnknownHostException e) {
            LOG.error("can not get localhost: ", e);
            return;
        }
        
        //  hard code, please modify to real supervisor address before mvn package
        Properties prop = new Properties();
        try {
            prop.load(getClass().getClassLoader().getResourceAsStream("connection.properties"));
        } catch (IOException e) {
            LOG.fatal("Load app.properties file fail.", e);
            return;
        }
        
        snapshotPersistDir = prop.getProperty("agent.snapshot.persist.dir", "/data/applogs/phoenix/snapshot");
        
        String supervisorHost = prop.getProperty("supervisor.host");
        int supervisorPort = Integer.parseInt(prop.getProperty("supervisor.port"));

        try {    
            listener = new FileListener();
        } catch (Exception e) {
            LOG.error("Failed to create a file listener, agent shutdown!", e);
            return;
        }
        
        processor = new AgentProcessor();
        client = new GenClient(
                processor,
                new SimpleConnection.SimpleConnectionFactory(),
                null);

        try {
            client.init("agent", supervisorHost, supervisorPort);
        } catch (ClosedChannelException e) {
            LOG.error(e.getMessage(), e);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } catch (Throwable t) {
            LOG.error(t.getMessage(), t);
        }
    }
    
    public void fillUpAppLogsFromConfig(TopicId topicId) {
        String topic = topicId.getTopic();
        String path = ConfigKeeper.configMap.get(topic).getString(ParamsKey.TopicConf.WATCH_FILE);
        long rotatePeroid = ConfigKeeper.configMap.get(topic).getLong(ParamsKey.TopicConf.ROTATE_PERIOD);
        long rollPeroid = ConfigKeeper.configMap.get(topic).getLong(ParamsKey.TopicConf.ROLL_PERIOD);
        int maxLineSize = ConfigKeeper.configMap.get(topic).getInteger(ParamsKey.TopicConf.MAX_LINE_SIZE, 512000);
        long readInterval = ConfigKeeper.configMap.get(topic).getLong(ParamsKey.TopicConf.READ_INTERVAL, 1L);
        TopicMeta topicMeta = new TopicMeta(topicId, path, rotatePeroid, rollPeroid, maxLineSize, readInterval);
        logMetas.put(topicId, topicMeta);
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
        processor.OnDisconnected(supervisor);
        recoveryThreadPool.shutdownNow();
        scheduler.shutdownNow();
        
        client.shutdown();
    }
    
    /**
     * Just for unit test
     **/
    public void setListener(FileListener listener) {
        this.listener = listener;
    }

    public void reportFailure(TopicId topicId, String source, final long ts) {
        Message message = PBwrap.wrapAppFailure(topicId.getTopic(), source, ts);
        send(message);
        TopicMeta applog = logMetas.get(topicId);
        topicReaders.remove(applog);
        register(topicId, applog.getCreateTime());
    }

    public void reportUnrecoverable(TopicId topicId, String source, final long rollPeriod, final long rollTs, boolean isFinal) {
        Message message = PBwrap.wrapUnrecoverable(topicId.getTopic(), source, rollPeriod, rollTs, isFinal);
        send(message);
    }

    public void reportRecoveryFail(TopicId topicId, String source, long period, final long rollTs, boolean isFinal) {
        Message message = PBwrap.wrapRecoveryFail(topicId.getTopic(), source, period, rollTs, isFinal);
        send(message);
    }

    public void removeRecoverying(TopicId topicId, final long rollTs) {
        String recoveryKey = topicId.toString() + ":" + rollTs;
        recoveryingMap.remove(recoveryKey);
    }
    
    public void send(Message message) {
        LOG.debug("send: " + message);
        Util.send(supervisor, message);
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
            if (!paasModel) {
                requireConfigFromSupersivor(0);
            }
        }

        @Override
        public void OnDisconnected(SimpleConnection connection) {
            confLoopFactor = 1;
            
            supervisor = null;
            
            for(Runnable task : scheduler.getQueue()) {
                scheduler.remove(task);//TODO not cancel clean
            }

            // close connected streams
            LOG.info("shutdown app node");
            for (java.util.Map.Entry<TopicMeta, LogReader> e : topicReaders.entrySet()) {
                LogReader reader = e.getValue();
                reader.stop();
                topicReaders.remove(e.getKey());
            }
            for (Map.Entry<String, RollRecovery> e : recoveryingMap.entrySet()) {
                RollRecovery recovery = e.getValue();
                recovery.stop();
                recoveryingMap.remove(e.getKey());
            }
            logMetas.clear();
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
            String topic;
            String broker;
            String instanceId;
            TopicId topicId;
            TopicMeta topicMeta = null;
            LogReader logReader = null;
            RollRecovery rollRecovery = null;
            
            MessageType type = msg.getType();
            Random random = new Random();
            switch (type) {
            case NOAVAILABLENODE:
                topic = msg.getNoAvailableNode().getTopic();
                instanceId = msg.getNoAvailableNode().getInstanceId();
                topicId = new TopicId(topic, instanceId);
                TopicMeta applog = logMetas.get(topicId);
                register(topicId, applog.getCreateTime());
                break;
            case RECOVERY_ROLL:
                RecoveryRoll recoveryRoll = msg.getRecoveryRoll();
                topic = recoveryRoll.getTopic();
                instanceId = recoveryRoll.getInstanceId();
                topicId = new TopicId(topic, instanceId);
                boolean isFinal = recoveryRoll.getIsFinal();
                if ((topicMeta = logMetas.get(topicId)) != null) {
                    LogReader reader = topicReaders.get(topicMeta);
                    if (reader == null) {
                        LOG.error("Can not find reader by " + topicId + " to recovery");
                        return false;
                    }
                    IState state = reader.getState();
                    long rollTs = recoveryRoll.getRollTs();
                    String recoveryKey = topicId.getContent() + ":" + rollTs;
                    if ((rollRecovery = recoveryingMap.get(recoveryKey)) == null) {
                        broker = recoveryRoll.getBrokerServer();
                        int recoveryPort = recoveryRoll.getRecoveryPort();
                        rollRecovery = new RollRecovery(Agent.this,
                                broker, recoveryPort, topicMeta, rollTs, isFinal, state);
                        recoveryingMap.put(recoveryKey, rollRecovery);
                        recoveryThreadPool.execute(rollRecovery);
                        return true;
                    } else {
                        LOG.info("duplicated recovery roll message: "
                                + recoveryRoll);
                    }
                } else {
                    LOG.error("AppName [" + recoveryRoll.getTopic()
                            + "] from supervisor message not match with local");
                }
                break;
            case ASSIGN_BROKER:
                AssignBroker assignBroker = msg.getAssignBroker();
                topic = assignBroker.getTopic();
                instanceId = assignBroker.getInstanceId();
                topicId = new TopicId(topic, instanceId);
                if ((topicMeta = logMetas.get(topicId)) != null) {
                    if ((logReader = topicReaders.get(topicMeta)) == null) {
                        if (topicMeta.isDying()) {
                            LOG.warn(topicMeta + " is dying, do not restart log reader.");
                            return true;
                        }
                        broker = assignBroker.getBrokerServer();
                        int brokerPort = assignBroker.getBrokerPort();
                        logReader = new LogReader(Agent.this, hostname, broker, brokerPort, topicMeta, snapshotPersistDir);
                        topicReaders.put(topicMeta, logReader);
                        pool.execute(logReader);
                        return true;
                    } else {
                        LOG.info("duplicated assign broker message: " + assignBroker);
                    }
                } else {
                    LOG.error("AppName [" + assignBroker.getTopic()
                            + "] from supervisor message not match with local");
                }
                break;
            case NOAVAILABLECONF:
                if (confLoopFactor < 20) {
                    confLoopFactor = confLoopFactor << 1;
                }
                int randomSecond = confLoopFactor * (random.nextInt(21) + 40);
                LOG.info("Configurations not ready, sleep " + randomSecond + " second.");
                requireConfigFromSupersivor(randomSecond);
                break;
            case CONF_RES:
                confLoopFactor = 1;
                ConfRes confRes = msg.getConfRes();
                if (isPaasModel()) {
                    LOG.info("paas model, receive conf response.");
                    List<LxcConfRes> lxcConfResList = confRes.getLxcConfResList();
                    for (LxcConfRes lxcConfRes : lxcConfResList) {
                        topic = lxcConfRes.getTopic();
                        List<String> ids = lxcConfRes.getInstanceIdsList();
                        for (String id : ids) {
                            topicId = new TopicId(topic, id);
                            if (logMetas.containsKey(topicId)) {
                                LOG.info(topicId + " has already in used.");
                                continue;
                            }
                            //check files existence
                            if (!checkFilesExist(topic, lxcConfRes.getWatchFile(), id)) {
                                continue;
                            }
                            confKeeper.addRawProperty(topic + "."
                                    + ParamsKey.TopicConf.ROTATE_PERIOD, lxcConfRes.getRotatePeriod());
                            confKeeper.addRawProperty(topic + "."
                                    + ParamsKey.TopicConf.ROLL_PERIOD, lxcConfRes.getRollPeriod());
                            confKeeper.addRawProperty(topic + "."
                                    + ParamsKey.TopicConf.MAX_LINE_SIZE, lxcConfRes.getMaxLineSize());
                            confKeeper.addRawProperty(topic + "."
                                    + ParamsKey.TopicConf.READ_INTERVAL, lxcConfRes.getReadInterval());
                            fillUpAppLogsFromConfig(topicId);
                            register(topicId, Util.getTS());
                            if (this.heartbeat == null || !this.heartbeat.isAlive()) {
                                this.heartbeat = new HeartBeat(supervisor);
                                this.heartbeat.setDaemon(true);
                                this.heartbeat.start();
                            }
                        }
                    }
                } else {
                    List<AppConfRes> appConfResList = confRes.getAppConfResList();
                    int accepted = 0;
                    for (AppConfRes appConfRes : appConfResList) {
                        topic = appConfRes.getTopic();
                        topicId = new TopicId(topic, null);
                        if (logMetas.containsKey(topicId)) {
                            LOG.info(topicId + " has already in used.");
                            ++accepted;
                            continue;
                        }
                        //check files existence
                        if (!checkFilesExist(topic, appConfRes.getWatchFile())) {
                            continue;
                        }
                        confKeeper.addRawProperty(topic + "."
                                + ParamsKey.TopicConf.ROTATE_PERIOD, appConfRes.getRotatePeriod());
                        confKeeper.addRawProperty(topic + "."
                                + ParamsKey.TopicConf.ROLL_PERIOD, appConfRes.getRollPeriod());
                        confKeeper.addRawProperty(topic + "."
                                + ParamsKey.TopicConf.MAX_LINE_SIZE, appConfRes.getMaxLineSize());
                        confKeeper.addRawProperty(topic + "."
                                + ParamsKey.TopicConf.READ_INTERVAL, appConfRes.getReadInterval());
                        fillUpAppLogsFromConfig(topicId);
                        ++accepted;
                        register(topicId, Util.getTS());
                        if (this.heartbeat == null || !this.heartbeat.isAlive()) {
                            this.heartbeat = new HeartBeat(supervisor);
                            this.heartbeat.setDaemon(true);
                            this.heartbeat.start();
                        }
                    }
                    if (accepted < appConfResList.size()) {
                        LOG.error("Not all configurations are accepted, sleep 5 minutes...");
                        requireConfigFromSupersivor(5 * 60);
                    }
                }
                break;
            case QUIT:
                Quit quit = msg.getQuit();
                List<InstanceGroup> instanceGroupQuitList = quit.getInstanceGroupList();
                for (InstanceGroup instanceGroup : instanceGroupQuitList) {
                    topic = instanceGroup.getTopic();
                    List<String> ids = instanceGroup.getInstanceIdsList();
                    for (String id : ids) {
                        topicId = new TopicId(topic, id);
                        if ((topicMeta = logMetas.get(topicId)) != null) {
                            // set a stream status to dying, and send a special rotate message.
                            if (topicMeta.setDying()) {
                                if (!new File(topicMeta.getTailFile()).exists()) {
                                    LOG.warn("QUIT but " + topicMeta.getTailFile() + " not exists, retire stream and trigger CLEAN.");
                                    send(PBwrap.wrapRetireStream(topic, hostname, id));
                                } else if ((logReader = topicReaders.get(topicMeta)) != null) {
                                    logReader.beginLastLogRotate();
                                } else {
                                    LOG.info(topicMeta + " has already stopped.");
                                }
                            } else {
                                LOG.info(topicId + " was dying.");
                            }
                        }
                    }
                }
                break;
            case CLEAN:
                Clean clean = msg.getClean();
                List<InstanceGroup> instanceGroupCleanList = clean.getInstanceGroupList();
                for (InstanceGroup instanceGroup : instanceGroupCleanList) {
                    topic = instanceGroup.getTopic();
                    List<String> ids = instanceGroup.getInstanceIdsList();
                    for (String id : ids) {
                        topicId = new TopicId(topic, id);
                        if ((topicMeta = logMetas.get(topicId)) != null) {
                            // set a stream status to dying, and send a special rotate message.
                            if (topicMeta.isDying()) {
                                if ((logReader = topicReaders.get(topicMeta)) != null) {
                                    LOG.info("Clean up " + topicMeta);
                                    logReader.stop();
                                    topicReaders.remove(topicMeta);
                                    logMetas.remove(topicId);
                                } else {
                                    LOG.info(topicMeta + " has already stopped.");
                                }
                            } else {
                                LOG.info(topicId + " was dying.");
                            }
                        }
                    }
                }
                break;
            default:
                LOG.error("Illegal message type " + msg.getType());
            }
            return false;
        }
    }
    
    public static void main(String[] args) {
        Agent agent;
        if (args.length > 0) {
            agent = new Agent(args[0]);
        } else {
            agent = new Agent();
        }
        Thread thread = new Thread(agent);
        thread.start();
    }
}

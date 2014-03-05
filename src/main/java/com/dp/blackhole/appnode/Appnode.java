package com.dp.blackhole.appnode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.collectornode.ControlMessageTypeFactory;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.gen.AssignCollectorPB.AssignCollector;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.MessagePB.Message.MessageType;
import com.dp.blackhole.common.gen.RecoveryRollPB.RecoveryRoll;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.network.EntityProcessor;
import com.dp.blackhole.network.GenClient;
import com.dp.blackhole.network.SimpleConnection;
import com.google.protobuf.InvalidProtocolBufferException;

public class Appnode {
    private static final Log LOG = LogFactory.getLog(Appnode.class);
    private String[] args;
    private File configFile = null;
    private ExecutorService pool;
    protected int recoveryPort;   //protected for test case SimAppnode
    private int brokerPort;
    private String hostname;
    private FileListener listener;
    private static Map<String, AppLog> appLogs = new ConcurrentHashMap<String, AppLog>();
    private static Map<AppLog, LogReader> appReaders = new ConcurrentHashMap<AppLog, LogReader>();
    
    private GenClient<ByteBuffer, SimpleConnection, AgentProcessor> client;
    private AgentProcessor processor;
    
    public Appnode(String hostname, String[] args) {
        this.hostname = hostname;
        this.args = args;
        pool = Executors.newCachedThreadPool();
    }
    
    private void register(String appName, long regTimestamp) {
        Message msg = PBwrap.wrapAppReg(appName, hostname, regTimestamp);
        send(msg);
    }

    public String getHost() {
        return hostname;
    }

    private boolean checkAllFilesExist() {
        boolean res = true;
        for (String appName : ConfigKeeper.configMap.keySet()) {
            String path = ConfigKeeper.configMap.get(appName)
                    .getString(ParamsKey.Appconf.WATCH_FILE);
            File fileForTest = new File(path);
            if (!fileForTest.exists()) {
                LOG.error("Appnode process start faild, because file " + path + " not found.");
                res = false;
            } else {
                LOG.info("Check file " + path + " ok.");
            }
        }
        return res;
    }

    public void run(Properties property) throws ClosedChannelException, IOException {
        if (!checkAllFilesExist()) {
            return;
        }
        fillUpAppLogsFromConfig();
        try {    
            listener = new FileListener();
        } catch (Exception e) {
            LOG.error("Failed to create a file listener, node shutdown!", e);
            return;
        }
        
        processor = new AgentProcessor();
        client = new GenClient(
                processor,
                new SimpleConnection.SimpleConnectionFactory(),
                new ControlMessageTypeFactory());

        client.init(property, "agent", "supervisor.host", "supervisor.port");
    }

    public void fillUpAppLogsFromConfig() {
        for (String appName : ConfigKeeper.configMap.keySet()) {
            String path = ConfigKeeper.configMap.get(appName)
                    .getString(ParamsKey.Appconf.WATCH_FILE);
            long rollPeroid = ConfigKeeper.configMap.get(appName)
                            .getLong(ParamsKey.Appconf.ROLL_PERIOD);
            AppLog appLog = new AppLog(appName, path, rollPeroid);
            appLogs.put(appName, appLog);
        }
    }


    private void registerApps() {
        //register the app to supervisor
        for (AppLog appLog : appLogs.values()) {
//            register(appLog.getAppName(), appLog.getCreateTime());
            register(appLog.getAppName(), Util.getTS());
        }
    }
    
	public void loadLionConfig() {
        // TODO Auto-generated method stub
    }

    public void loadLocalConfig() {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(configFile));
            Properties properties = new Properties();
            properties.load(reader);
            ConfigKeeper conf = new ConfigKeeper();
            Enumeration<?> propertyNames = properties.propertyNames();
            while (propertyNames.hasMoreElements()) {
                String name = (String) propertyNames.nextElement();
                String value = properties.getProperty(name);
            
                if (!conf.addRawProperty(name, value)) {
                    LOG.warn("Configuration property ignored: " + name + " = " + value);
                    continue;
                }
            }
        } catch (IOException e) {
                 LOG.error("Unable to load file:" + configFile
                                     + " (I/O failure) - Exception follows.", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ex) {
                    LOG.warn(
                            "Unable to close file reader for file: " + configFile, ex);
                }
            }
        }
    }

    public Properties loadConfig() throws FileNotFoundException, IOException {
        if (configFile != null) {
            loadLocalConfig();
        } else {
            loadLionConfig();
        }
        Properties prop = new Properties();
        prop.load(new FileReader(new File("config.properties")));
        recoveryPort = Integer.parseInt(prop.getProperty("broker.recovery.port"));      
        brokerPort = Integer.parseInt(prop.getProperty("broker.service.port"));
        
        return prop;
    }

    public boolean parseOptions() throws ParseException {
        Options options = new Options();
    
        Option option = new Option("f", "conf", true, "specify a conf file");
        options.addOption(option);
    
        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
    
        if (commandLine.hasOption('f')) {
            configFile = new File(commandLine.getOptionValue('f'));
    
            if (!configFile.exists()) {
                String path = configFile.getPath();
                try {
                    path = configFile.getCanonicalPath();
                } catch (IOException ex) {
                    LOG.error("Failed to read canonical path for file: " + path, ex);
                }
                throw new ParseException(
                        "The specified configuration file does not exist: " + path);
            }
        }
    
        return true;
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

    private void send(Message message) {
        processor.send(message);
    }
    
    public class AgentProcessor implements EntityProcessor<ByteBuffer, SimpleConnection> {
        private SimpleConnection supervisor;
        
        @Override
        public void OnConnected(SimpleConnection connection) {
            supervisor = connection;
            registerApps();
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
        }

        @Override
        public void process(ByteBuffer reply, SimpleConnection from) {

            Message msg = null;
            try {
                msg = PBwrap.Buf2PB(reply);
            } catch (InvalidProtocolBufferException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            LOG.debug("agent received: " + msg);
            
            String appName;
            String broker;
            AppLog appLog = null;
            LogReader logReader = null;
            MessageType type = msg.getType();
            switch (type) {
            case NOAVAILABLENODE:
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    LOG.info("thead interrupted");
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
                    broker = recoveryRoll.getCollectorServer();
                    RollRecovery recovery = new RollRecovery(Appnode.this,
                            broker, recoveryPort, appLog, rollTs);
                    pool.execute(recovery);
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
                        broker = assignCollector.getCollectorServer();
                        logReader = new LogReader(Appnode.this, hostname, broker, brokerPort,
                                appLog);
                        appReaders.put(appLog, logReader);
                        pool.execute(logReader);
                    } else {
                        LOG.info("duplicated assign collector message: "
                                + assignCollector);
                    }
                } else {
                    LOG.error("AppName [" + assignCollector.getAppName()
                            + "] from supervisor message not match with local");
                }
                break;
            default:
                LOG.error("Illegal message type " + msg.getType());
            }
        }

        public void send(Message message) {
            supervisor.send(PBwrap.PB2Buf(message));
        }
    }
    
    public static void main(String[] args) {
        String hostname;
        try {
            hostname = Util.getLocalHost();
        } catch (UnknownHostException e1) {
            LOG.error("Oops, got an exception:", e1);
            return;
        }
        Appnode appnode = new Appnode(hostname, args);

        try {
            if (appnode.parseOptions()) {
                Properties prop = appnode.loadConfig();
                appnode.run(prop);
            }
        } catch (ParseException e) {
            LOG.error("Oops, got an exception:", e);
        } catch (IOException e) {
            LOG.error("Can not load file \"config.properties\"", e);
        } catch (Exception e) {
            LOG.error("A fatal error occurred while running. Exception follows.", e);
        }
    }
}

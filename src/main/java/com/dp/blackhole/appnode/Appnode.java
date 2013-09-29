package com.dp.blackhole.appnode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
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

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.common.gen.AssignCollectorPB.AssignCollector;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.common.gen.MessagePB.Message.MessageType;
import com.dp.blackhole.common.gen.RecoveryRollPB.RecoveryRoll;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.node.Node;

public class Appnode extends Node {
    private static final Log LOG = LogFactory.getLog(Appnode.class);
    private String[] args;
    private File configFile = null;
    private ExecutorService exec;
    private int port;
    private static Map<String, AppLog> appLogs = new ConcurrentHashMap<String, AppLog>();
    private static Map<AppLog, LogReader> appReaders = new ConcurrentHashMap<AppLog, LogReader>();
    
    public Appnode(String appClient) {
        exec = Executors.newCachedThreadPool();
    }

    public boolean process(Message msg) {
	    String appName;
	    String collectorServer;
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
                collectorServer = recoveryRoll.getCollectorServer();
                RollRecovery recovery = new RollRecovery(this, collectorServer, port, appLog, rollTs);
                exec.execute(recovery);
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
                    logReader = new LogReader(this, collectorServer, port, appLog);
                    appReaders.put(appLog, logReader);
                    exec.execute(logReader);
                    return true;
                } else {
                    LOG.info("duplicated assign collector message: " + assignCollector);
                }
            } else {
                LOG.error("AppName [" + assignCollector.getAppName()
                        + "] from supervisor message not match with local");
            }
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
                LOG.error("Appnode process start faild, because file " + path + " not found.");
                res = false;
            } else {
                LOG.info("Check file " + path + " ok.");
            }
        }
        return res;
    }

    public void run() {
        if (!checkAllFilesExist()) {
            return;
        }
        fillUpAppLogsFromConfig();

        //wait for receiving message from supervisor
        super.loop();
    }

    public void fillUpAppLogsFromConfig() {
        for (String appName : ConfigKeeper.configMap.keySet()) {
            String path = ConfigKeeper.configMap.get(appName)
                    .getString(ParamsKey.Appconf.WATCH_FILE);
            AppLog appLog = new AppLog(appName, path);
            appLogs.put(appName, appLog);
        }
    }

    @Override
    protected void onDisconnected() {
        // close connected streams
        for (java.util.Map.Entry<AppLog, LogReader> e : appReaders.entrySet()) {
            LogReader reader = e.getValue();
            reader.stop();
            appReaders.remove(e.getKey());
        }
    }
    
    @Override
    protected void onConnected() {
        clearMessageQueue();
        registerApps();
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

    public void loadConfig() throws FileNotFoundException, IOException {
        if (configFile != null) {
            loadLocalConfig();
        } else {
            loadLionConfig();
        }
        Properties prop = new Properties();
        prop.load(new FileReader(new File("config.properties")));
        port = Integer.parseInt(prop.getProperty("collectornode.port"));
        
        String serverhost = prop.getProperty("supervisor.host");
        int serverport = Integer.parseInt(prop.getProperty("supervisor.port"));
        init(serverhost, serverport);    
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

    public String[] getArgs() {
        return args;
    }

    public void setArgs(String[] args) {
        this.args = args;
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

    public static void main(String[] args) {
        String hostname;
        try {
            hostname = Util.getLocalHost();
        } catch (UnknownHostException e1) {
            LOG.error("Oops, got an exception:", e1);
            return;
        }
        Appnode appnode = new Appnode(hostname);
        appnode.setArgs(args);

        try {
            if (appnode.parseOptions()) {
                appnode.loadConfig();
                appnode.run();
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

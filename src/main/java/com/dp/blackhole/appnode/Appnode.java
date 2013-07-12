package com.dp.blackhole.appnode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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

import com.dp.blackhole.common.AppRegPB.AppReg;
import com.dp.blackhole.common.AppRollPB.AppRoll;
import com.dp.blackhole.common.AssignCollectorPB.AssignCollector;
import com.dp.blackhole.common.MessagePB.Message;
import com.dp.blackhole.common.MessagePB.Message.MessageType;
import com.dp.blackhole.common.RecoveryCollectorPB.RecoveryCollector;
import com.dp.blackhole.common.RecoveryRollPB.RecoveryRoll;
import com.dp.blackhole.conf.BasicConfigurationConstants;
import com.dp.blackhole.conf.Configuration;
import com.dp.blackhole.node.Node;

public class Appnode extends Node {
  private static final Log LOG = LogFactory.getLog(Appnode.class);
  private String[] args;
  private File configFile = null;
  private ExecutorService exec;
  private String appClient;
  private static Map<String, AppLog> appLogs = Collections.synchronizedMap(new HashMap<String, AppLog>());
  private static Map<String, LogReader> appReaders = Collections.synchronizedMap(new HashMap<String, LogReader>());
  public Appnode(String appClient) {
    this.appClient = appClient;
  }

	public void process(Message msg) {
	  String tmpAppName;
	  AppLog tmpAppLog;
	  MessageType type = msg.getType();
	  switch (type) {
    case RECOVERY_COLLECTOR:
      RecoveryCollector recoveryCollector = msg.getRecoveryCollector();
      tmpAppName = recoveryCollector.getAppName();
      if (appReaders.containsKey(tmpAppName)) {
        LogReader reader = appReaders.get(tmpAppName);
        reader.stop();
        reader = null;
        tmpAppLog = appLogs.get(tmpAppName);
        tmpAppLog.setServer(recoveryCollector.getCollectorServer());
        tmpAppLog.setPort(recoveryCollector.getCollectorPort());
        reader = new LogReader(this, tmpAppLog, true);
        exec.execute(reader);
      } else {
        LOG.error("AppName [" + recoveryCollector.getAppName()
            + "] from supervisor message not match with local");
      }
      break;
    case RECOVERY_ROLL:
      RecoveryRoll recoveryRoll = msg.getRecoveryRoll();
      tmpAppName = recoveryRoll.getAppName();
      if (appReaders.containsKey(tmpAppName)) {
        String rollIdent = recoveryRoll.getRollIdent();
        long offset = recoveryRoll.getOffset();
        tmpAppLog = appLogs.get(tmpAppName);
        tmpAppLog.setServer(recoveryRoll.getCollectorServer());
        tmpAppLog.setPort(recoveryRoll.getCollectorPort());
        LogLoader loader = new LogLoader(this, tmpAppLog, rollIdent, offset);
        exec.execute(loader);
      } else {
        LOG.error("AppName [" + recoveryRoll.getAppName()
            + "] from supervisor message not match with local");
      }
      break;
    case ASSIGN_COLLECTOR:
      AssignCollector assignCollector = msg.getAssignCollector();
      tmpAppName = assignCollector.getAppName();
      if (appLogs.containsKey(tmpAppName)) {
        tmpAppLog = appLogs.get(tmpAppName);
        tmpAppLog.setServer(assignCollector.getCollectorServer());
        tmpAppLog.setPort(assignCollector.getCollectorPort());
        
        LogReader logReader = new LogReader(this, tmpAppLog, false);
        appReaders.put(tmpAppLog.getAppName(), logReader);
        exec.execute(logReader);
      } else {
        LOG.error("AppName [" + assignCollector.getAppName()
            + "] from supervisor message not match with local");
      }
      break;
    default:
      break;
    }
	}

  public void roll(String appName, String rollIdent, long fileLength) {
    AppRoll.Builder appRollBuilder = AppRoll.newBuilder();
    appRollBuilder.setAppName(appName);
    appRollBuilder.setAppServer(appClient);
    appRollBuilder.setRollIdent(rollIdent);
    appRollBuilder.setLength(fileLength);
    AppRoll appRoll = appRollBuilder.build();
    super.sendMessage(wrapMessage(appRoll));
  }

  /**
   * Wrap all kinds of event to a common message to transfer.
   * @param object
   * @return
   */
  private Message wrapMessage(Object object) {
    Message.Builder messageBuilder = Message.newBuilder();
    if (object instanceof AppReg) {
      messageBuilder.setType(MessageType.APP_REG);
      messageBuilder.setAppReg((AppReg)object);
    } else {
      messageBuilder.setType(MessageType.APP_ROLL);
      messageBuilder.setAppRoll((AppRoll)object);
    }
    Message message = messageBuilder.build();
    return message;
  }

  public void register(String appName, long regTimestamp) {
    AppReg.Builder appRegBuilder = AppReg.newBuilder();
    appRegBuilder.setAppName(appName);
    appRegBuilder.setAppServer(appClient);
    appRegBuilder.setRegTs(regTimestamp);
    AppReg appReg = appRegBuilder.build();
    super.sendMessage(wrapMessage(appReg));
  }

  private boolean checkAllFilesExist() {
    boolean res = true;
    for (String appName : Configuration.appConfigMap.keySet()) {
      String path = Configuration.appConfigMap.get(appName)
          .getString(BasicConfigurationConstants.WATCHDIR);
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
    exec = Executors.newCachedThreadPool();
    if (!checkAllFilesExist()) {
      return;
    }
    for (String appName : Configuration.appConfigMap.keySet()) {
      String path = Configuration.appConfigMap.get(appName)
          .getString(BasicConfigurationConstants.WATCHDIR);
      AppLog appLog = new AppLog(appName, path);
      appLogs.put(appName, appLog);
      //register the app to supervisor
      register(appName, appLog.getCreateTime());
      //wait for receiving message from supervisor
    }
    super.loop();
  }

	private void loadLionConfig() {
    // TODO Auto-generated method stub
  }

  private void loadLocalConfig() {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(configFile));
      Properties properties = new Properties();
      properties.load(reader);
      Configuration conf = new Configuration();
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

  private void loadConfig() {
    if (configFile != null) {
      loadLocalConfig();
    } else {
      loadLionConfig();
    }
  }

  private boolean parseOptions() throws ParseException {
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

  public static void main(String[] args) {
    String client;
    try {
      client = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e1) {
      LOG.error(e1);
      return;
    }
    Appnode appnode = new Appnode(client);
    appnode.setArgs(args);

    try {
      if (appnode.parseOptions()) {
        appnode.loadConfig();
        appnode.run();
      }
    } catch (ParseException e) {
      LOG.error(e.getMessage());
    } catch (Exception e) {
      LOG.error("A fatal error occurred while running. Exception follows.",
          e);
    }
  }

  public String[] getArgs() {
    return args;
  }

  public void setArgs(String[] args) {
    this.args = args;
  }
}

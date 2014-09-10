package com.dp.blackhole.supervisor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dianping.lion.client.ConfigChange;
import com.dianping.lion.client.LionException;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.conf.Context;

public class ConfigManager {
    private static final Log LOG = LogFactory.getLog(ConfigManager.class);

    public final Set<String> topicSet = new CopyOnWriteArraySet<String>();
    
    private final Map<String, Set<String>> hostToTopics = Collections.synchronizedMap(new HashMap<String, Set<String>>());
    private final Map<String, List<String>> topicToHosts = Collections.synchronizedMap(new HashMap<String, List<String>>());
    
    private final Map<String, Set<String>> cmdbAppToTopics = Collections.synchronizedMap(new HashMap<String, Set<String>>());
    private final Map<String, String> topicToCmdb = Collections.synchronizedMap(new HashMap<String, String>());
    
    private final ConfigCache cache;
    private final Supervisor supervisor;
    
    private int apiId;
    
    public int webServicePort;
    public int connectionTimeout;
    public int socketTimeout;
    
    public int supervisorPort;
    public int numHandler;
    
    public String getPaaSInstanceURLPerfix;
    
    ConfigManager(Supervisor supervisor) throws LionException {
        this.cache = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
        this.supervisor = supervisor;
    }
    
    public Supervisor getSupervisor() {
        return this.supervisor;
    }
    
    public Set<String> getTopicsByHost(String host) {
        return hostToTopics.get(host);
    }
    
    public Set<String> getTopicsByCmdb(String cmdbApp) {
        return cmdbAppToTopics.get(cmdbApp);
    }
    
    public String getCmdbAppByTopic(String topic) {
        return topicToCmdb.get(topic);
    }
    
    public void initConfig() throws IOException {
        Properties prop = new Properties();
        prop.load(ClassLoader.getSystemResourceAsStream("config.properties"));
        apiId = Integer.parseInt(prop.getProperty("supervisor.lionapi.id"));
        webServicePort = Integer.parseInt(prop.getProperty("supervisor.webservice.port"));
        connectionTimeout = Integer.parseInt(prop.getProperty("supervisor.webservice.connectionTimeout", "30000"));
        socketTimeout = Integer.parseInt(prop.getProperty("supervisor.webservice.socketTimeout", "10000"));
        
        supervisorPort = Integer.parseInt(prop.getProperty("supervisor.port"));;
        numHandler = Integer.parseInt(prop.getProperty("GenServer.handler.count", "3"));
        getPaaSInstanceURLPerfix = prop.getProperty("supervisor.paas.url");
        
        reloadTopicConfig();
    }
    
    private void reloadTopicConfig() {
        reloadConf();
        addWatchers();
    }
    
    private void reloadConf() {
        String topicsString = definitelyGetProperty(ParamsKey.LionNode.TOPIC);
        String[] topics = Util.getStringListOfLionValue(topicsString);
        if (topics == null || topics.length == 0) {
            LOG.info("There are no legacy configurations.");
            return;
        }
        for (int i = 0; i < topics.length; i++) {
            topicSet.add(topics[i]);
            String confString = definitelyGetProperty(ParamsKey.LionNode.CONF_PREFIX + topics[i]);
            if (confString == null) {
                LOG.error("Lose configurations for " + topics[i]);
            }
            fillConfMap(topics[i], confString);
            String hostsString = definitelyGetProperty(ParamsKey.LionNode.HOSTS_PREFIX + topics[i]);
            if (hostsString == null) {
                LOG.error("Lose hosts for " + topics[i]);
            }
            fillHostMap(topics[i], hostsString);
            String cmdbString = definitelyGetProperty(ParamsKey.LionNode.CMDB_PREFIX + topics[i]);
            if (cmdbString == null) {
                LOG.error("Lose CMDB mapping for " + topics[i]);
            }
            fillCMDBMap(topics[i], cmdbString);
        }
    }

    private void addWatchers() {
        TopicsChangeListener topicsListener = new TopicsChangeListener();
        cache.addChange(topicsListener);
        TopicConfChangeListener topicConfListener = new TopicConfChangeListener();
        cache.addChange(topicConfListener);
        AgentHostsChangeListener agentHostsListener = new AgentHostsChangeListener();
        cache.addChange(agentHostsListener);
        definitelyGetProperty(ParamsKey.LionNode.TOPIC);
    }

    private synchronized String definitelyGetProperty(String watchKey) {
        while (true) {
            try {
                String value = cache.getProperty(watchKey);
                LOG.info("add watcher for " + watchKey);
                return value;
            } catch (LionException e) {
                LOG.warn(e.getMessage(), e);
                LOG.info("reset watcher to " + watchKey + "after 3 sencond...");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e1) {
                    LOG.error(e1.getMessage(), e1);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private synchronized void fillConfMap(String topic, String confValue) {
        String[][] confKV = Util.getStringMapOfLionValue(confValue);
        if (confKV != null) {
            boolean isPaaSModel = false;
            for (int i = 0; i < confKV.length; i++) {
                String key = confKV[i][0];
                String value = confKV[i][1];
                LOG.info("topic:" + topic + " K:" + key + " V:" + value);
                if (ConfigKeeper.configMap.containsKey(topic)) {
                    ConfigKeeper.configMap.get(topic).put(key, value);
                } else {
                    ConfigKeeper.configMap.put(topic, new Context(key, value));
                }
                if (key.equals("isPaaS") && value.equals("true")) {
                    isPaaSModel = true;
                }
            }
            if (isPaaSModel) {
                supervisor.handleImportNewTopic(topic);
            }
        }
    }
    
    private synchronized void fillHostMap(String topic, String hostsValue) {
        String[] hosts = Util.getStringListOfLionValue(hostsValue);
        if (hosts != null) {
            List<String> list = new CopyOnWriteArrayList<String>();
            for (int i = 0; i < hosts.length; i++) {
                String host = hosts[i].trim();
                if (host.length() != 0) {
                    list.add(host);
                }
            }
            topicToHosts.put(topic, list);
            for (int i = 0; i < hosts.length; i++) {
                String host = hosts[i].trim();
                if (host.length() == 0) {
                    continue;
                }
                Set<String> topicsInOneHost;
                if ((topicsInOneHost = hostToTopics.get(host)) == null) {
                    topicsInOneHost = new CopyOnWriteArraySet<String>();
                    hostToTopics.put(host, topicsInOneHost);
                }
                topicsInOneHost.add(topic);
            }
        }
    }
    
    private synchronized void fillCMDBMap(String topic, String cmdbValue) {
        String cmdbApp = Util.getStringOfLionValue(cmdbValue);
        if (cmdbApp != null && cmdbApp.length() != 0) {
            topicToCmdb.put(topic, cmdbApp);
            Set<String> topicsInOneCMDB;
            if ((topicsInOneCMDB = cmdbAppToTopics.get(cmdbApp)) == null) {
                topicsInOneCMDB = new CopyOnWriteArraySet<String>();
                cmdbAppToTopics.put(cmdbApp, topicsInOneCMDB);
            }
            topicsInOneCMDB.add(topic);
        }
    }

    public String dumpconf() {
        StringBuilder sb = new StringBuilder();
        sb.append("dumpconf:\n");
        sb.append("############################## dump ##############################\n");
        sb.append("print topic configurations in MEMORY\n");
        for (String topic : topicSet) {
            sb.append("TOPIC: [").append(topic).append("]\n");
            sb.append("HOSTS: \n");
            List<String> hosts =  topicToHosts.get(topic);
            if (hosts != null) {
                for (String host : hosts) {
                    sb.append(host)
                    .append(" ");
                }
            }
            sb.append("\n")
            .append("CONF:\n");
            Context confContext;
            if ((confContext = ConfigKeeper.configMap.get(topic)) != null) {
                sb.append(ParamsKey.TopicConf.WATCH_FILE)
                .append(" = ")
                .append(confContext.getString(ParamsKey.TopicConf.WATCH_FILE, "null"))
                .append("\n")
                .append(ParamsKey.TopicConf.ROLL_PERIOD)
                .append(" = ")
                .append(confContext.getString(ParamsKey.TopicConf.ROLL_PERIOD, "3600"))
                .append("\n")
                .append(ParamsKey.TopicConf.MAX_LINE_SIZE)
                .append(" = ")
                .append(confContext.getString(ParamsKey.TopicConf.MAX_LINE_SIZE, "65536"))
                .append("\n");
            }
            sb.append("\n");
        }
        sb.append("##################################################################");
        
        return sb.toString();
    }
    
    public void removeConf(String topic, List<String> agentServers) {
        if (topicSet.contains(topic)) {
            List<String> hostsOfOneTopic = topicToHosts.get(topic);
            if (agentServers.isEmpty()) {
                topicSet.remove(topic);
                topicToHosts.remove(topic);
                ConfigKeeper.configMap.remove(topic);
                if (hostsOfOneTopic != null) {
                    for (String host : hostsOfOneTopic) {
                        Set<String> topicsInOneHost = hostToTopics.get(host);
                        if (topicsInOneHost.remove(topic)) {
                            LOG.info("remove "+ topic + " form hostToTopics for " + host);
                        } else {
                            LOG.warn(topic + " in topicsInOneHost had been removed before.");
                        }
                    }
                }
            } else {
                for (String agent : agentServers) {
                    Set<String> topicsInOneHost = hostToTopics.get(agent);
                    if (topicsInOneHost.remove(topic)) {
                        LOG.info("remove "+ topic + " form hostToTopics for " + agent);
                    } else {
                        LOG.error("Could not find topic: " + topic + " in topicsInOneHost. It should not happen.");
                    }
                    int index = indexOf(agent, hostsOfOneTopic);
                    if (index != -1) {
                        hostsOfOneTopic.remove(index);
                        LOG.info("remove agent " + agent + " form topicToHosts for " + topic);
                    } else {
                        LOG.error("Could not find server: " + agent + " in hostsOfOneTopic. It should not happen.");
                    }
                }
            }
        }
    }
    
    private int indexOf(String needToRemove, List<String> list) {
        if (list == null) {
            return -1;
        }
        int index = 0;
        for (String element : list) {
            if (needToRemove.equals(element))
                return index;
            index++;
        }
        return -1;
    }

    public String generateGetURL(String key) {
        return ParamsKey.LionNode.DEFAULT_LION_HOST +
                ParamsKey.LionNode.LION_GET_PATH +
                generateURIPrefix() +
                "&k=" + key;
    }

    public String generateSetURL(String key, String value) {
        String encodedValue = "";
        try {
            encodedValue = URLEncoder.encode(value,"UTF-8");
        } catch (UnsupportedEncodingException e) {
        }
        return ParamsKey.LionNode.DEFAULT_LION_HOST +
                ParamsKey.LionNode.LION_SET_PATH +
                generateURIPrefix() +
                "&ef=1" +
                "&k=" + key +
                "&v=" + encodedValue;
    }

    private String generateURIPrefix() {
        return "?&p=" + ParamsKey.LionNode.LION_PROJECT +
                "&e=" + EnvZooKeeperConfig.getEnv() +
                "&id=" + this.apiId;
    }

    class TopicsChangeListener implements ConfigChange {
    
        @Override
        public void onChange(String key, String value) {
            if (key.equals(ParamsKey.LionNode.TOPIC)) {
                String[] topics = Util.getStringListOfLionValue(value);
                if (topics != null) {
                    Set<String> newTopicSet = new HashSet<String>(Arrays.asList(topics));
                    for (String newTopic : newTopicSet) {
                        if (!topicSet.contains(newTopic)) {
                            LOG.info("Topics Change is triggered by "+ newTopic);
                            topicSet.add(newTopic);
                            String watchKey = ParamsKey.LionNode.CONF_PREFIX + newTopic;
                            addWatherForKey(watchKey);
                            watchKey = ParamsKey.LionNode.HOSTS_PREFIX + newTopic;
                            addWatherForKey(watchKey);
                        }
                    }
                    for (String oldTopic : topicSet) {
                        if (!newTopicSet.contains(oldTopic)) {
                            removeConf(oldTopic, new ArrayList<String>());
                        }
                    }
                }
            }
        }
    
        private void addWatherForKey(String watchKey) {
            definitelyGetProperty(watchKey);
        }
    }
    
    class CMDBChangeListener implements ConfigChange {

        @Override
        public void onChange(String key, String value) {
            if (key.startsWith(ParamsKey.LionNode.CMDB_PREFIX)) {
                for (String topic : topicSet) {
                    if (key.equals(ParamsKey.LionNode.CMDB_PREFIX + topic)) {
                        LOG.info("CMDB Change is triggered by " + topic);
                        fillCMDBMap(topic, value);
                        break;
                    }
                }
            }
        }
    }

    class AgentHostsChangeListener implements ConfigChange {

        @Override
        public void onChange(String key, String value) {
            if (key.startsWith(ParamsKey.LionNode.HOSTS_PREFIX)) {
                for (String topic : topicSet) {
                    if (key.equals(ParamsKey.LionNode.HOSTS_PREFIX + topic)) {
                        LOG.info("Agent Hosts Change is triggered by " + topic);
                        fillHostMap(topic, value);
                        break;
                    }
                }
            }
        }
    }

    class TopicConfChangeListener implements ConfigChange {
    
        @Override
        public void onChange(String key, String value) {
            if (key.startsWith(ParamsKey.LionNode.CONF_PREFIX)) {
                for (String topic : topicSet) {
                    if (key.equals(ParamsKey.LionNode.CONF_PREFIX + topic)) {
                        LOG.info("Topic Conf Change is triggered by " + topic);
                        fillConfMap(topic, value);
                        break;
                    }
                }
            }
        }
    }
}
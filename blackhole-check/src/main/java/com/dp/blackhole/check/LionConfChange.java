package com.dp.blackhole.check;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dianping.lion.client.ConfigChange;
import com.dianping.lion.client.LionException;

public class LionConfChange {
    private static final Log LOG = LogFactory.getLog(LionConfChange.class);

    public Set<String> topicSet = new CopyOnWriteArraySet<String>();
    
    private final Map<String, Set<String>> hostToTopics = new ConcurrentHashMap<String, Set<String>>();
    private final Map<String, List<String>> topicToHosts = new ConcurrentHashMap<String, List<String>>();
    private Set<String> topicBlacklist;
    private Set<String> alarmBlackList;
    private Set<String> skipSourceBlackList;
    private ConfigCache cache;
    private int apiId;
    private final ScheduledThreadPoolExecutor scheduler;
    
    LionConfChange(ConfigCache cache, int apiId) {
        this.cache = cache;
        this.apiId = apiId;
        scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }
    
    public Map<String, List<String>> getTopicToHostsMap() {
        return topicToHosts;
    }

    public Set<String> getTopicsByHost(String host) {
        return hostToTopics.get(host);
    }
    
    public synchronized Set<String> getTopicBlacklist() {
        return topicBlacklist;
    }
    
    public synchronized Set<String> getAlarmBlackList() {
        return alarmBlackList;
    }
    
    public synchronized Set<String> getSkipSourceBlackList() {
        return skipSourceBlackList;
    }

    public void initLion() {
        reloadConf();
        addWatchers();
    }
    private void reloadConf() {
        String blacklistString = definitelyGetProperty(ParamsKey.LionNode.BLACKLIST);
        String alarmBlacklistString = definitelyGetProperty(ParamsKey.LionNode.ALARM_BLACKLIST);
        String skipSourceBlacklistString = definitelyGetProperty(ParamsKey.LionNode.SKIP_BLACKLIST);
        String[] blacklistArray = Util.getStringListOfLionValue(blacklistString);
        String[] alarmBlacklistArray = Util.getStringListOfLionValue(alarmBlacklistString);
        String[] skipSourceBlacklistArray = Util.getStringListOfLionValue(skipSourceBlacklistString);
        synchronized (this) {
            topicBlacklist = new CopyOnWriteArraySet<String>(Arrays.asList(blacklistArray));
            alarmBlackList = new CopyOnWriteArraySet<String>(Arrays.asList(alarmBlacklistArray));
            skipSourceBlackList = new CopyOnWriteArraySet<String>(Arrays.asList(skipSourceBlacklistArray));
        }
        
        String topicsString = definitelyGetProperty(ParamsKey.LionNode.TOPIC);
        String[] topicArray = Util.getStringListOfLionValue(topicsString);
        if (topicArray == null || topicArray.length == 0) {
            LOG.info("There are no legacy configurations of topic.");
            return;
        }
        for (int i = 0; i < topicArray.length; i++) {
            topicSet.add(topicArray[i]);//all with blacklist
            String confString = definitelyGetProperty(ParamsKey.LionNode.TOPIC_CONF_PREFIX + topicArray[i]);
            if (confString == null) {
                LOG.error("Lose configurations for " + topicArray[i]);
            }
            fillConfMap(topicArray[i], confString);
            String hostsString = definitelyGetProperty(ParamsKey.LionNode.TOPIC_HOSTS_PREFIX + topicArray[i]);
            if (hostsString == null) {
                LOG.error("Lose hosts for " + topicArray[i]);
            }
            fillHostMap(topicArray[i], hostsString);
        }
    }

    private void addWatchers() {
        LionChangeListener listener = new LionChangeListener();
        cache.addChange(listener);
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
            for (int i = 0; i < confKV.length; i++) {
                String key = confKV[i][0];
                String value = confKV[i][1];
                LOG.info("topic:" + topic + " K:" + key + " V:" + value);
                if (ConfigKeeper.configMap.containsKey(topic)) {
                    ConfigKeeper.configMap.get(topic).put(key, value);
                } else {
                    ConfigKeeper.configMap.put(topic, new Context(key, value));
                }
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
    
    public String dumpconf() {
        StringBuilder sb = new StringBuilder();
        sb.append("dumpconf:\n");
        sb.append("############################## dump ##############################\n");
        sb.append("print app configurations in MEMORY\n");
        for (String appname : topicSet) {
            sb.append("TOPIC: [").append(appname).append("]\n");
            sb.append("HOSTS: \n");
            List<String> hosts =  topicToHosts.get(appname);
            if (hosts != null) {
                for (String host : hosts) {
                    sb.append(host)
                    .append(" ");
                }
            }
            sb.append("\n")
            .append("CONF:\n");
            Context confContext;
            if ((confContext = ConfigKeeper.configMap.get(appname)) != null) {
                sb.append(ParamsKey.TopicConfig.WATCH_FILE)
                .append(" = ")
                .append(confContext.getString(ParamsKey.TopicConfig.WATCH_FILE, "null"))
                .append("\n")
                .append(ParamsKey.TopicConfig.ROLL_PERIOD)
                .append(" = ")
                .append(confContext.getString(ParamsKey.TopicConfig.ROLL_PERIOD, "3600"))
                .append("\n")
                .append(ParamsKey.TopicConfig.MAX_LINE_SIZE)
                .append(" = ")
                .append(confContext.getString(ParamsKey.TopicConfig.MAX_LINE_SIZE, "65536"))
                .append("\n");
            }
            sb.append("\n");
        }
        sb.append("##################################################################");
        
        return sb.toString();
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
    
    public void removeConf(String topic, List<String> appServers) {
        if (topicSet.contains(topic)) {
            List<String> hostsOfOneTopic = topicToHosts.get(topic);
            if (appServers.isEmpty()) {
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
                for (String server : appServers) {
                    Set<String> topicsInOneHost = hostToTopics.get(server);
                    if (topicsInOneHost.remove(topic)) {
                        LOG.info("remove "+ topic + " form hostToTopics for " + server);
                    } else {
                        LOG.error("Could not find topic: " + topic + " in topicsInOneHost. It should not happen.");
                    }
                    int index = indexOf(server, hostsOfOneTopic);
                    if (index != -1) {
                        hostsOfOneTopic.remove(index);
                        LOG.info("remove server " + server + " form topicToHosts for " + topic);
                    } else {
                        LOG.error("Could not find server: " + server + " in hostsOfOneTopic. It should not happen.");
                    }
                }
            }
        }
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
    
    class LionChangeListener implements ConfigChange {
        
        private void addWatherForKey(String watchKey) {
            definitelyGetProperty(watchKey);
        }
        private void addChecker(final String topic) {
            Context context = ConfigKeeper.configMap.get(topic);
            if (context == null) {
                LOG.error("Can not get topic: " + topic + " from configMap now, try in 10 seconds later.");
                scheduler.schedule(new Runnable() {
                    @Override
                    public void run() {
                        addChecker(topic);
                    }
                }, 10, TimeUnit.SECONDS);
                return;
            }
            RollIdent ident = new RollIdent();
            ident.period = Long.parseLong(context.getString(ParamsKey.TopicConfig.ROLL_PERIOD));
            ident.topic = topic;
            ident.kvmSources = topicToHosts.get(topic);
            ident.ts = Util.getCurrWholeTs(new Date().getTime(), ident.period);
            ident.timeout = -1;
            CheckDone checker = new CheckDone(ident);
            LOG.info("create a checkdone thread " + checker.toString());
            ScheduledFuture<?> scheduledFuture = CheckDone.checkerThreadPool.scheduleWithFixedDelay(checker, 0, CheckDone.checkperiod, TimeUnit.SECONDS);
            CheckDone.threadMap.put(ident.topic, scheduledFuture);
        }
        
        @Override
        public void onChange(String key, String value) {
            if (key.equals(ParamsKey.LionNode.BLACKLIST)) {
                String[] blacklistArray = Util.getStringListOfLionValue(value);
                LOG.info("black list has been changed.");
                synchronized (LionConfChange.this) {
                    topicBlacklist = new CopyOnWriteArraySet<String>(Arrays.asList(blacklistArray));
                }
            } else if (key.equals(ParamsKey.LionNode.ALARM_BLACKLIST)) {
                String[] alarmBlacklistArray = Util.getStringListOfLionValue(value);
                LOG.info("alarm black list has been changed.");
                synchronized (LionConfChange.this) {
                    alarmBlackList = new CopyOnWriteArraySet<String>(Arrays.asList(alarmBlacklistArray));
                }
            } else if (key.equals(ParamsKey.LionNode.SKIP_BLACKLIST)) {
                String[] skipBlacklistArray = Util.getStringListOfLionValue(value);
                LOG.info("skip black list has been changed.");
                synchronized (LionConfChange.this) {
                    skipSourceBlackList = new CopyOnWriteArraySet<String>(Arrays.asList(skipBlacklistArray));
                }
            } else if (key.equals(ParamsKey.LionNode.TOPIC)) {
                String[] topics = Util.getStringListOfLionValue(value);
                Set<String> newTopicSet = new HashSet<String>(Arrays.asList(topics));
                for (String newTopic : newTopicSet) {
                    if (!topicSet.contains(newTopic)) {
                        LOG.info("Topic changed: " + newTopic + " added.");
                        topicSet.add(newTopic);
                        String watchKey = ParamsKey.LionNode.TOPIC_CONF_PREFIX + newTopic;
                        addWatherForKey(watchKey);
                        watchKey = ParamsKey.LionNode.TOPIC_HOSTS_PREFIX + newTopic;
                        addWatherForKey(watchKey);
                    }
                }
                for (String oldTopic : topicSet) {
                    if (!newTopicSet.contains(oldTopic)) {
                        removeConf(oldTopic, new ArrayList<String>());
                        //remove checker
                        LOG.info("Topic changed: " + oldTopic + " removed.");
                        ScheduledFuture<?> scheduledFuture = CheckDone.threadMap.get(oldTopic);
                        scheduledFuture.cancel(false);
                    }
                }
            } else if (key.startsWith(ParamsKey.LionNode.TOPIC_HOSTS_PREFIX)) {
                for (String topic : topicSet) {
                    if (key.equals(ParamsKey.LionNode.TOPIC_HOSTS_PREFIX + topic)) {
                        LOG.info("Topic Hosts Change is triggered by " + topic);
                        fillHostMap(topic, value);
                        //add checker
                        if(!CheckDone.threadMap.containsKey(topic)) {
                            addChecker(topic);
                        }
                        break;
                    }
                }
            } else if (key.startsWith(ParamsKey.LionNode.TOPIC_CONF_PREFIX)) {
                for (String topicName : topicSet) {
                    if (key.equals(ParamsKey.LionNode.TOPIC_CONF_PREFIX + topicName)) {
                        LOG.info("Topic Config Change is triggered by " + topicName);
                        fillConfMap(topicName, value);
                        break;
                    }
                }
            } else {
                LOG.warn("unknow lion key change: " + key);
            }
        }
    }
}
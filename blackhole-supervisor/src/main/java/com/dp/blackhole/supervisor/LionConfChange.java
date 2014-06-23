package com.dp.blackhole.supervisor;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

public class LionConfChange {
    private static final Log LOG = LogFactory.getLog(LionConfChange.class);

    public final Set<String> appSet = new CopyOnWriteArraySet<String>();
    
    private final Map<String, Set<String>> hostToAppNames = Collections.synchronizedMap(new HashMap<String, Set<String>>());
    private final Map<String, List<String>> appToHosts = Collections.synchronizedMap(new HashMap<String, List<String>>());
    
    private final Map<String, Set<String>> cmdbToAppNames = Collections.synchronizedMap(new HashMap<String, Set<String>>());

    private ConfigCache cache;
    private int apiId;
    
    LionConfChange(ConfigCache cache, int apiId) {
        this.cache = cache;
        this.apiId = apiId;
    }
    
    public Set<String> getAppNamesByHost(String host) {
        return hostToAppNames.get(host);
    }
    
    public Set<String> getAppNamesByCmdb(String cmdbApp) {
        return cmdbToAppNames.get(cmdbApp);
    }
    
    public void initLion() {
        reloadConf();
        addWatchers();
    }
    private void reloadConf() {
        String appNamesString = definitelyGetProperty(ParamsKey.LionNode.APPS);
        String[] appNames = Util.getStringListOfLionValue(appNamesString);
        if (appNames == null || appNames.length == 0) {
            LOG.info("There are no legacy configurations.");
            return;
        }
        for (int i = 0; i < appNames.length; i++) {
            appSet.add(appNames[i]);
            String confString = definitelyGetProperty(ParamsKey.LionNode.APP_CONF_PREFIX + appNames[i]);
            if (confString == null) {
                LOG.error("Lose configurations for " + appNames[i]);
            }
            fillConfMap(appNames[i], confString);
            String hostsString = definitelyGetProperty(ParamsKey.LionNode.APP_HOSTS_PREFIX + appNames[i]);
            if (hostsString == null) {
                LOG.error("Lose hosts for " + appNames[i]);
            }
            fillHostMap(appNames[i], hostsString);
            String cmdbString = definitelyGetProperty(ParamsKey.LionNode.APP_CMDB_PREFIX + appNames[i]);
            if (cmdbString == null) {
                LOG.error("Lose CMDB mapping for " + appNames[i]);
            }
            fillCMDBMap(appNames[i], cmdbString);
        }
    }

    private void addWatchers() {
        AppsChangeListener appsListener = new AppsChangeListener();
        cache.addChange(appsListener);
        AppConfChangeListener appConfListener = new AppConfChangeListener();
        cache.addChange(appConfListener);
        AppHostsChangeListener appHostsListener = new AppHostsChangeListener();
        cache.addChange(appHostsListener);
        definitelyGetProperty(ParamsKey.LionNode.APPS);
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

    private synchronized void fillConfMap(String appName, String confValue) {
        String[][] confKV = Util.getStringMapOfLionValue(confValue);
        if (confKV != null) {
            for (int i = 0; i < confKV.length; i++) {
                String key = confKV[i][0];
                String value = confKV[i][1];
                LOG.info("appName:" + appName + " K:" + key + " V:" + value);
                if (ConfigKeeper.configMap.containsKey(appName)) {
                    ConfigKeeper.configMap.get(appName).put(key, value);
                } else {
                    ConfigKeeper.configMap.put(appName, new Context(key, value));
                }
            }
        }
    }
    
    private synchronized void fillHostMap(String appName, String hostsValue) {
        String[] hosts = Util.getStringListOfLionValue(hostsValue);
        if (hosts != null) {
            List<String> list = new CopyOnWriteArrayList<String>();
            for (int i = 0; i < hosts.length; i++) {
                list.add(hosts[i]);
            }
            appToHosts.put(appName, list);
            for (int i = 0; i < hosts.length; i++) {
                String host = hosts[i].trim();
                if (host.length() == 0) {
                    continue;
                }
                Set<String> appNamesInOneHost;
                if ((appNamesInOneHost = hostToAppNames.get(host)) == null) {
                    appNamesInOneHost = new CopyOnWriteArraySet<String>();
                    hostToAppNames.put(host, appNamesInOneHost);
                }
                appNamesInOneHost.add(appName);
            }
        }
    }
    
    private synchronized void fillCMDBMap(String appName, String cmdbValue) {
        String[] cmdbApps = Util.getStringListOfLionValue(cmdbValue);
        if (cmdbApps != null) {
            for (int i = 0; i < cmdbApps.length; i++) {
                String cmdb = cmdbApps[i].trim();
                if (cmdb.length() == 0) {
                    continue;
                }
                Set<String> appNamesInOneCMDB;
                if ((appNamesInOneCMDB = cmdbToAppNames.get(cmdb)) == null) {
                    appNamesInOneCMDB = new CopyOnWriteArraySet<String>();
                    cmdbToAppNames.put(cmdb, appNamesInOneCMDB);
                }
                appNamesInOneCMDB.add(appName);
            }
        }
    }

    public String dumpconf() {
        StringBuilder sb = new StringBuilder();
        sb.append("dumpconf:\n");
        sb.append("############################## dump ##############################\n");
        sb.append("print app configurations in MEMORY\n");
        for (String appname : appSet) {
            sb.append("APP: [").append(appname).append("]\n");
            sb.append("HOSTS: \n");
            List<String> hosts =  appToHosts.get(appname);
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
                sb.append(ParamsKey.Appconf.WATCH_FILE)
                .append(" = ")
                .append(confContext.getString(ParamsKey.Appconf.WATCH_FILE, "null"))
                .append("\n")
                .append(ParamsKey.Appconf.ROLL_PERIOD)
                .append(" = ")
                .append(confContext.getString(ParamsKey.Appconf.ROLL_PERIOD, "3600"))
                .append("\n")
                .append(ParamsKey.Appconf.MAX_LINE_SIZE)
                .append(" = ")
                .append(confContext.getString(ParamsKey.Appconf.MAX_LINE_SIZE, "65536"))
                .append("\n");
            }
            sb.append("\n");
        }
        sb.append("##################################################################");
        
        return sb.toString();
    }
    
    public void removeConf(String appName, List<String> appServers) {
        if (appSet.contains(appName)) {
            List<String> hostsOfOneApp = appToHosts.get(appName);
            if (appServers.isEmpty()) {
                appSet.remove(appName);
                appToHosts.remove(appName);
                ConfigKeeper.configMap.remove(appName);
                if (hostsOfOneApp != null) {
                    for (String host : hostsOfOneApp) {
                        Set<String> appNamesInOneHost = hostToAppNames.get(host);
                        if (appNamesInOneHost.remove(appName)) {
                            LOG.info("remove "+ appName + " form hostToAppNames for " + host);
                        } else {
                            LOG.warn(appName + " in appNamesInOneHost had been removed before.");
                        }
                    }
                }
            } else {
                for (String server : appServers) {
                    Set<String> appNamesInOneHost = hostToAppNames.get(server);
                    if (appNamesInOneHost.remove(appName)) {
                        LOG.info("remove "+ appName + " form hostToAppNames for " + server);
                    } else {
                        LOG.error("Could not find app: " + appName + " in appNamesInOneHost. It should not happen.");
                    }
                    int index = indexOf(server, hostsOfOneApp);
                    if (index != -1) {
                        hostsOfOneApp.remove(index);
                        LOG.info("remove server " + server + " form appToHosts for " + appName);
                    } else {
                        LOG.error("Could not find server: " + server + " in hostsOfOneApp. It should not happen.");
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

    class AppsChangeListener implements ConfigChange {
    
        @Override
        public void onChange(String key, String value) {
            if (key.equals(ParamsKey.LionNode.APPS)) {
                String[] appNames = Util.getStringListOfLionValue(value);
                if (appNames != null) {
                    Set<String> newAppSet = new HashSet<String>(Arrays.asList(appNames));
                    for (String newApp : newAppSet) {
                        if (!appSet.contains(newApp)) {
                            LOG.info("Apps Change is triggered by "+ newApp);
                            appSet.add(newApp);
                            String watchKey = ParamsKey.LionNode.APP_CONF_PREFIX + newApp;
                            addWatherForKey(watchKey);
                            watchKey = ParamsKey.LionNode.APP_HOSTS_PREFIX + newApp;
                            addWatherForKey(watchKey);
                        }
                    }
                    for (String oldApp : appSet) {
                        if (!newAppSet.contains(oldApp)) {
                            removeConf(oldApp, new ArrayList<String>());
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
            if (key.startsWith(ParamsKey.LionNode.APP_CMDB_PREFIX)) {
                for (String appName : appSet) {
                    if (key.equals(ParamsKey.LionNode.APP_CMDB_PREFIX + appName)) {
                        LOG.info("CMDB Change is triggered by " + appName);
                        fillCMDBMap(appName, value);
                        break;
                    }
                }
            }
        }
    }

    class AppHostsChangeListener implements ConfigChange {

        @Override
        public void onChange(String key, String value) {
            if (key.startsWith(ParamsKey.LionNode.APP_HOSTS_PREFIX)) {
                for (String appName : appSet) {
                    if (key.equals(ParamsKey.LionNode.APP_HOSTS_PREFIX + appName)) {
                        LOG.info("App Hosts Change is triggered by " + appName);
                        fillHostMap(appName, value);
                        break;
                    }
                }
            }
        }
    }

    class AppConfChangeListener implements ConfigChange {
    
        @Override
        public void onChange(String key, String value) {
            if (key.startsWith(ParamsKey.LionNode.APP_CONF_PREFIX)) {
                for (String appName : appSet) {
                    if (key.equals(ParamsKey.LionNode.APP_CONF_PREFIX + appName)) {
                        LOG.info("App Conf Change is triggered by " + appName);
                        fillConfMap(appName, value);
                        break;
                    }
                }
            }
        }
    }
}
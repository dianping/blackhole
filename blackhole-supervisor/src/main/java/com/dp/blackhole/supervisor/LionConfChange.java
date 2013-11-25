package com.dp.blackhole.supervisor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dianping.lion.client.ConfigCache;
import com.dianping.lion.client.ConfigChange;
import com.dianping.lion.client.LionException;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.conf.Context;

public class LionConfChange {
    private static final Log LOG = LogFactory.getLog(LionConfChange.class);

    final Map<String, List<String>> hostToAppNames = new ConcurrentHashMap<String, List<String>>();
    private final Map<String, List<String>> appToHosts = new ConcurrentHashMap<String, List<String>>();
    private final Set<String> appSet = new CopyOnWriteArraySet<String>();

    private ConfigCache cache;
    
    LionConfChange(ConfigCache cache) {
        this.cache = cache;
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
            appToHosts.put(appName, Arrays.asList(hosts));
            for (int i = 0; i < hosts.length; i++) {
                if (hosts[i].trim().length() == 0) {
                    continue;
                }
                List<String> appNamesInOneHost;
                if ((appNamesInOneHost = hostToAppNames.get(hosts[i].trim())) == null) {
                    appNamesInOneHost = new ArrayList<String>();
                    hostToAppNames.put(hosts[i].trim(), appNamesInOneHost);
                }
                appNamesInOneHost.add(appName);
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
    
    class AppsChangeListener implements ConfigChange {
    
        @Override
        public void onChange(String key, String value) {
            if (key.equals(ParamsKey.LionNode.APPS)) {
                String[] appNames = Util.getStringListOfLionValue(value);
                for (int i = 0; i < appNames.length; i++) {
                    if (appSet.contains(appNames[i])) {
                        continue;
                    }
                    LOG.info("Apps Change is triggered by "+ appNames[i]);
                    appSet.add(appNames[i]);
                    String watchKey = ParamsKey.LionNode.APP_CONF_PREFIX + appNames[i];
                    addWatherForKey(watchKey);
                    watchKey = ParamsKey.LionNode.APP_HOSTS_PREFIX + appNames[i];
                    addWatherForKey(watchKey);
                }
            }
        }
    
        private void addWatherForKey(String watchKey) {
            definitelyGetProperty(watchKey);
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
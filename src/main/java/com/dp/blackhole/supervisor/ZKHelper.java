package com.dp.blackhole.supervisor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.conf.Context;

/**
 * ZKHelper is a singleton class which wrapped zookeeper client(zkCli).
 * It provides based functions like zkCli and exception handling.
 * ZKHekper also play a role of watcher, it execute some actions triggered by zk events.
 * Additional, It has some business codes which work with zkCli.
 */
public class ZKHelper implements Watcher{
    private static final Log LOG = LogFactory.getLog(ZKHelper.class);
    private ZooKeeper zkClient;
    private String connectString;
    private int sessionTimeout;
    private int appNumber;
    private Set<String> appSet;
    ConcurrentHashMap<String, ArrayList<String>> appHostToAppNames;
    private ZKHelper() {
    }

    private static class SingletonHolder {
        private static ZKHelper instance = new ZKHelper();
    }
    
    public static final ZKHelper getInstance() {
        return SingletonHolder.instance;
    }

    public void createConnection(String connectString, int sessionTimeout) throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, this);
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
    }
    public void reConnect() throws IOException {
        if (zkClient != null && zkClient.getState().isAlive()) {
            return;
        }
        zkClient = new ZooKeeper(connectString, sessionTimeout, this);
    }
    public void releaseConnection() {
        try {
            if (zkClient != null) {
                zkClient.close();
            }
        } catch (InterruptedException e) {
            //TODO
            e.printStackTrace();
        }
    }
    
    private void addZNode(String path, String data, CreateMode createMode) 
            throws KeeperException, InterruptedException {
        zkClient.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
    }
    
    private void addSequentialZNode(String path, String data) 
            throws KeeperException, InterruptedException {
        zkClient.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT_SEQUENTIAL);
    }

    public void delZNode(String path) 
            throws InterruptedException, KeeperException{
         zkClient.delete(path, -1);
    }

    public List listZNodes(String path) 
            throws KeeperException, InterruptedException {
        List children = null;
        children = zkClient.getChildren(path, false);
        return children;
    }
 
    private void setZNodeData(String path, String data) 
            throws KeeperException, InterruptedException {
        Stat stat = zkClient.setData(path, data.getBytes(), -1);
    }
    
    public String getZNodeData(String path) 
            throws KeeperException, InterruptedException {
        String data;
        byte[] byteData = zkClient.getData(path, false, null);
        data = new String(byteData);
        return data;
    }
    
    /**
     * 
     * @param path
     * @return if return true means two possibility: node not exist or left watch failed
     * @throws InterruptedException 
     * @throws KeeperException 
     */
    public boolean leftWatcher(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return (zkClient.exists(path, watcher) == null) ? false : true;
    }
    
    public void leftChildrenWatcher(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        zkClient.getChildren(path, watcher, stat);
    }
    
    public boolean notExist(String path) {
        try {
            return !leftWatcher(path, null);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }
    
    @Override
    public void process(WatchedEvent event) {
        //TODO
        String path = event.getPath();
        LOG.info("watcher triggered by " + path + ", event type:" + event.getType().name());
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
            case SyncConnected:
                // In this particular example we don't need to do anything
                // here - watches are automatically re-registered with 
                // server and any watches triggered while the client was 
                // disconnected will be delivered (in order of course)
                LOG.info("zookeeper sync connected.");
                break;
            case Expired:
                // It's all over
                LOG.warn("Zookeeper session expired, rebuilding connection.. Watch out!");
                try {
                    reConnect();
                } catch (IOException e) {
                    LOG.error("reconnect failed!", e);
                }
                break;
            }
        } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
            try {
                if (path.substring(0, path.lastIndexOf('/')).equals(ParamsKey.ZNode.CONFS)) {//is appconf
                    Stat stat = new Stat();
                    leftChildrenWatcher(path, this, stat);  //re-left a watcher on appconf node
                    if (stat.getNumChildren() > appNumber) {    //add a new appconf node
                        List<String> children = listZNodes(path);
                        loadZKData(children);
                        
                    } else {    //delete a appconf node
                        //TODO what should we do? send a message to shutdown associated logReader thread?
                    }
                }
            } catch (KeeperException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else if (event.getType() == Event.EventType.NodeDeleted) {
            //we don't left a watcher on any appconf node, so their change will not get here.
            LOG.warn("Watch out! Detect a NodeDelete event of " + path);
        } else {
            // Something has changed on the node, let's find out
            LOG.warn("We not handle this event, please check!");
        }
    }

    /**
     * rawAddZnode means add a znode without exception handle
     */
    public void rawAddZNode(String path, String data) 
            throws KeeperException, InterruptedException {
        addZNode(path, data, CreateMode.PERSISTENT);
    }

    public String getKey(String content) {
        return content.substring(0, content.indexOf('=') - 1);
    }
    public String getValue(String content) {
        return content.substring(content.indexOf('=') + 1);
    }
    
    public void loadAllConfs() throws KeeperException, InterruptedException, IOException {
        List<String> children;
        try {
            /** if there is no confs node in zookeeper tree, 
             * throw an IOException we define here.
             * If left watcher on confs node failed, 
             * the supervisor will catch exception in initializeZK(). */
            leftChildrenWatcher(ParamsKey.ZNode.CONFS, this, null);
        } catch (Exception e) {
            throw new IOException("There is no confs node \"/blackhole/confs\" in zookeeper tree");
        }
        children = listZNodes(ParamsKey.ZNode.CONFS);
        if (children.size() == 0) {
            throw new IOException("There is no app node under \"/blackhole/confs\", please add manually");
        }
        appNumber = children.size();
        appSet = new HashSet<String>();
        loadZKData(children);
    }

    public void loadZKData(List<String> children) throws KeeperException,
            InterruptedException {
        for (String appName : children) {
            if (appSet.contains(appName)) {
                continue;
            }
            appSet.add(appName);
            String appConfPath = ParamsKey.ZNode.CONFS + "/" + appName;
            String[] confLines = getZNodeData(appConfPath).split("\n");
            for (String confLine : confLines) {
                if (confLine.length() == 0) {
                    continue;   //deal with empty line
                }
                String key = getKey(confLine).trim();
                String value = getValue(confLine).trim();
                if (key.equals(ParamsKey.Appconf.APP_HOSTS)) {
                    String[] hosts = value.split(";");
                    for (int i = 0; i < hosts.length; i++) {
                        if (appHostToAppNames.get(hosts[i].trim()) == null) {
                            ArrayList<String> appNamesInOneHost = new ArrayList<String>();
                            appNamesInOneHost.add(appName);
                            appHostToAppNames.put(hosts[i].trim(), appNamesInOneHost);
                        } else {
                            appHostToAppNames.get(hosts[i].trim()).add(appName);
                        }
                    }
                }
                LOG.info("appName:" + appName + " K:" + key + " V:" + value);
                if (ConfigKeeper.configMap.containsKey(appName)) {
                    ConfigKeeper.configMap.get(appName).put(key, value);
                } else {
                    ConfigKeeper.configMap.put(appName, new Context(key, value));
                }
            }
        }
    }
}

package com.dp.blackhole.agent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.contentobjects.jnotify.IJNotify;
import net.contentobjects.jnotify.JNotifyException;
import net.contentobjects.jnotify.JNotifyListener;

import com.dp.blackhole.common.Util;

public class FileListener implements JNotifyListener{
    private static final Log LOG = LogFactory.getLog(FileListener.class);
    public static final int FILE_CREATED    = 0x1;
    public static final int FILE_DELETED    = 0x2;
    public static final int FILE_MODIFIED   = 0x4;
    public static final int FILE_RENAMED    = 0x8;
    public static final int FILE_ANY        = FILE_CREATED | FILE_DELETED | FILE_MODIFIED | FILE_RENAMED;
    private IJNotify iJNotifyInstance;
    private ConcurrentHashMap<String, LogReader> readerMap;
    private Set<String> parentWathchPathSet;
    private Map<String, Integer> path2wd;
    // Lock the path2wd map only to avoid non-atomic operation
    private Lock lock = new ReentrantLock();

    public FileListener() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        iJNotifyInstance = (IJNotify) Class.forName("net.contentobjects.jnotify.linux.JNotifyAdapterLinux").newInstance();
        readerMap = new ConcurrentHashMap<String, LogReader>();
        //guarantee by synchronized
        parentWathchPathSet = new HashSet<String>();
        path2wd = new HashMap<String, Integer>();
    }

    public synchronized boolean registerLogReader(final String watchPath, final LogReader reader) {
        int fwd, wd;
        if (readerMap.putIfAbsent(watchPath, reader) == null) {
            String parentPath = Util.getParentAbsolutePath(watchPath);
            if (!parentWathchPathSet.contains(parentPath)) {
                parentWathchPathSet.add(parentPath);
                try {
                    fwd = iJNotifyInstance.addWatch(parentPath, FILE_CREATED, false, this);
                } catch (JNotifyException e) {
                    LOG.error("Failed to add watch for " + parentPath, e);
                    readerMap.remove(watchPath);
                    parentWathchPathSet.remove(parentPath);
                    return false;
                }
                lock.lock();
                try {
                    path2wd.put(parentPath, fwd);
                } finally {
                    lock.unlock();
                }
                LOG.info("Registerring and monitoring parent path " + parentPath + " \"FILE_CREATE\"");
            } else {
                LOG.info("Watch parent path " + parentPath + " has already exist in the Set");
            }
            
            try {
                wd = iJNotifyInstance.addWatch(watchPath, FILE_MODIFIED, false, this);
            } catch (JNotifyException e) {
                LOG.error("Failed to add watch for " + watchPath, e);
                readerMap.remove(watchPath);
                return false;
            }
            
            //tag file appending
            reader.doFileAppendForce();
            lock.lock();
            try {
                path2wd.put(watchPath, wd);
            } finally {
                lock.unlock();
            }
            LOG.info("Registerring and monitoring tail file " + watchPath + " \"FILE_MODIFIED\"");
        } else {
            LOG.info("Watch path " + watchPath + " has already exist in the Map");
        }
        return true;
    }
    
    public void unregisterLogReader(final String watchPath, final LogReader reader) {
        Integer wd;
        lock.lock();
        try {
            if ((wd = path2wd.get(watchPath)) == null) {
                return;
            } else {
                path2wd.remove(watchPath);
            }
        } finally {
            lock.unlock();
        }
        try {
            iJNotifyInstance.removeWatch(wd);
            reader.resetCurrentLogStatus();
            LOG.info("Unregister watch path " + watchPath);
        } catch (JNotifyException e) {
            LOG.fatal("Failed to remove wd " + wd + " for " + watchPath + 
                    ". Because the watch descriptor wd is not valid;" +
                    " or fd is not an inotify file descriptor." +
                    " See \"inotify_rm_watch\" for more detail", e);
        }
        readerMap.remove(watchPath);
    }

    /**
     * trigger by file create in parent path
     * if created file is which we watched,
     * remove associated wd from inotify and
     * add watch again (also persistent new wd).
     */
    @Override
    public void fileCreated(int wd, String rootPath, String name) {
        String createdFilePath = rootPath + "/" + name;
        LogReader reader;
        if ((reader = readerMap.get(createdFilePath)) != null) {
            LOG.info("rotate detected of " + createdFilePath);
            reader.beginLogRotate();
            //Here, we lock to removing the old and adding a new path as a atomic operation.
            lock.lock();
            try {
                Integer oldWd;
                if ((oldWd = path2wd.get(createdFilePath)) != null) {
                    iJNotifyInstance.removeWatch(oldWd);
                    path2wd.remove(createdFilePath);
                } else {
                    LOG.fatal("Failed to get wd by file " + createdFilePath);
                }
                Integer newWd = iJNotifyInstance.addWatch(createdFilePath, FILE_MODIFIED, false, this);
                path2wd.put(createdFilePath, newWd);
                LOG.info("Re-monitoring "+ createdFilePath + " \"FILE_MODIFIED\" for rotate.");
            } catch (JNotifyException e) {
                LOG.fatal("Failed to add or remove watch for " + createdFilePath, e);
            } finally {
                lock.unlock();
            }
        }
        else {
            LOG.info("create file " + createdFilePath + " is not in reader map");
        }
    }

    @Override
    public void fileDeleted(int wd, String rootPath, String name) {
    }

    @Override
    public void fileModified(int wd, String rootPath, String name) {
        LogReader reader; 
        if ((reader = readerMap.get(rootPath)) != null ) {
            reader.doFileAppend();
        }
    }

    @Override
    public void fileRenamed(int wd, String rootPath, String oldName,
            String newName) {
    }
}
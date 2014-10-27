package com.dp.blackhole.http;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.supervisor.Supervisor;

public abstract class HttpAbstractHandler {
    public static long TIMEOUT = 120000L;
    public static final long CHECK_PERIOD = 10000L;
    public static final String SUCCESS = "SUCCESS";
    public static final String NONEED = "NONEED";
    public static final String FAILURE = "FAILURE";
    public abstract HttpResult getContent(String app, String[] ... args);
    
    protected Map<String, Set<String>> extractIdMapShuffleByHost(String[] ids, String[] ips) {
        Map<String, Set<String>> hostIds = new HashMap<String, Set<String>>();
        for (int i = 0; i< ips.length; i++) {
            InetAddress host;
            try {
                host = InetAddress.getByName(ips[i]);
            } catch (UnknownHostException e) {
                continue;
            }
            Set<String> idsInTheSameHost;
            if ((idsInTheSameHost = hostIds.get(host.getHostName())) == null) {
                idsInTheSameHost = new HashSet<String>();
                hostIds.put(host.getHostName(), idsInTheSameHost);
            }
            idsInTheSameHost.add(ids[i]);
        }
        return hostIds;
    }
    
    protected void filterHost(String topic, String host, Set<String> idsInTheSameHost, boolean expect, Supervisor supervisor) {
        Iterator<String> it = idsInTheSameHost.iterator();
        while(it.hasNext()){
            String ids = it.next();
            if(expect == supervisor.isActiveStream(topic, Util.getSource(host, ids))){
                it.remove();
            }
        }
    }
    
    protected boolean inBlacklist(String topic) {
        // TODO Auto-generated method stub
        return false;
    }
}

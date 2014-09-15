package com.dp.blackhole.http;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.supervisor.Supervisor;

public abstract class HttpAbstractHandler {
    public static long TIMEOUT = 120000L;
    public static final long CHECK_PERIOD = 10000L;
    public static final String SUCCESS = "SUCCESS";
    public static final String NONEED = "NONEED";
    public static final String FAILURE = "FAILURE";
    public abstract HttpResult getContent(String app, String[] ... args);
    
    protected Map<String, List<String>> extractIdMapShuffleByHost(String[] ids, String[] ips) {
        Map<String, List<String>> hostIds = new HashMap<String, List<String>>();
        for (int i = 0; i< ips.length; i++) {
            InetAddress host;
            try {
                host = InetAddress.getByName(ips[i]);
            } catch (UnknownHostException e) {
                continue;
            }
            List<String> idsInTheSameHost;
            if ((idsInTheSameHost = hostIds.get(host.getHostName())) == null) {
                idsInTheSameHost = new LinkedList<String>();
                hostIds.put(host.getHostName(), idsInTheSameHost);
            }
            idsInTheSameHost.add(ids[i]);
        }
        return hostIds;
    }
    
    protected void filterHost(String topic, String host, List<String> idsInTheSameHost, boolean expect, Supervisor supervisor) {
        for (int i = 0; i < idsInTheSameHost.size(); i++) {
            if (expect == supervisor.isActiveStream(topic, Util.getSourceIdentify(host, idsInTheSameHost.get(i)))) {
                idsInTheSameHost.remove(i);
            }
        }
    }
    
    protected boolean inBlacklist(String topic) {
        // TODO Auto-generated method stub
        return false;
    }
}

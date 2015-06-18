package com.dp.blackhole.check;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class GetInstanceFromPaas extends Thread {
    private final static Log LOG = LogFactory.getLog(GetInstanceFromPaas.class);
    private long sleepDuration;
    private List<RollIdent> rollIdents;
    private boolean running = true;
    private HttpClientSingle httpClient;
    private Set<String> currentBlacklist;

    public String getPaaSInstanceURLPerfix;
    public GetInstanceFromPaas(long sleepDuration, List<RollIdent> rollIdents, String getPaaSInstanceURLPerfix) {
        this.sleepDuration = sleepDuration;
        this.rollIdents = rollIdents;
        this.getPaaSInstanceURLPerfix = getPaaSInstanceURLPerfix;
        this.httpClient = new HttpClientSingle();
    }

    @Override
    public void run() {
        while (this.running)
            try {
                currentBlacklist = new HashSet<String>(CheckDone.lionConfChange.getTopicBlacklist());
                checkPaasSource();
                currentBlacklist = null;
                Thread.sleep(this.sleepDuration);
            } catch (InterruptedException e) {
                this.running = false;
            }
    }

    public void checkPaasSource() throws InterruptedException {
        for (RollIdent ident : rollIdents) {
            if (currentBlacklist.contains(ident.topic)) {
                continue;
            }
            Thread.sleep(10);
            Map<String, Set<String>> hostToInstances = new HashMap<String, Set<String>>();
             try {
                 hostToInstances = findInstancesByCmdbApps(ident.topic,ident.cmdbapp);
             } catch (Exception e) {
                 LOG.error("findInstances failed for " + ident.topic + " " + ident.cmdbapp, e);
                 continue;
             }
             List<String> sourceList = new ArrayList<String>();
             for (Map.Entry<String, Set<String>> entry : hostToInstances.entrySet()) {
                 String host = entry.getKey();
                 for (String instance : hostToInstances.get(host)) {
                     sourceList.add(host + '#' + instance);
                 }
             }
             ident.paasSources = sourceList;
        }
    }

    public Map<String, Set<String>> findInstancesByCmdbApps(String topic,
            List<String> cmdbApps) throws JSONException, IOException {
        Map<String, Set<String>> hostToInstances = new HashMap<String, Set<String>>();
        if (cmdbApps.size() == 0) {
            return hostToInstances;
        }
        // HTTP get the json
        StringBuilder catalogURLBuild = new StringBuilder();
        catalogURLBuild.append(getPaaSInstanceURLPerfix);
        for (String app : cmdbApps) {
            catalogURLBuild.append(app).append(",");
        }
        catalogURLBuild.deleteCharAt(catalogURLBuild.length() - 1);
        String response = httpClient
                .getResponseText(catalogURLBuild.toString());
        if (response == null) {
            LOG.error("response is null.");
            return hostToInstances;
        }
        String jsonStr = java.net.URLDecoder.decode(response, "utf-8");
        JSONObject rootObject = new JSONObject(jsonStr);
        JSONArray catalogArray = rootObject.getJSONArray("catalog");
        if (catalogArray.length() == 0) {
            LOG.warn("Can not get any catalog by url "
                    + catalogURLBuild.toString());
        }
        for (int i = 0; i < catalogArray.length(); i++) {
            JSONObject layoutObject = catalogArray.getJSONObject(i);
            String app = (String) layoutObject.get("app");
            JSONArray layoutArray = layoutObject.getJSONArray("layout");
            if (layoutArray.length() == 0) {
                LOG.debug("Can not get any layout by app " + app);
            }
            for (int j = 0; j < layoutArray.length(); j++) {
                JSONObject hostIdObject = layoutArray.getJSONObject(j);
                String ip = (String) hostIdObject.get("ip");
                InetAddress host;
                try {
                    host = InetAddress.getByName(ip);
                } catch (UnknownHostException e) {
                    LOG.error("Receive a unknown host " + ip, e);
                    continue;
                }
                String agentHost = host.getHostName();
                Set<String> instances;
                if ((instances = hostToInstances.get(agentHost)) == null) {
                    instances = new HashSet<String>();
                    hostToInstances.put(agentHost, instances);
                }
                JSONArray idArray = hostIdObject.getJSONArray("id");
                if (idArray.length() == 0) {
                    LOG.warn("Can not get any instance by host " + agentHost);
                }
                for (int k = 0; k < idArray.length(); k++) {
                    LOG.debug("PaaS catalog app: " + app + ", ip: " + ip
                            + ", id: " + idArray.getString(k));
                    instances.add(idArray.getString(k));
                }
            }
        }
        return hostToInstances;
    }
}
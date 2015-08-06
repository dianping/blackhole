package com.dp.blackhole.datachecker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.rest.ServiceFactory;
import com.dp.blackhole.supervisor.Supervisor;
import com.dp.blackhole.supervisor.model.Stage;
import com.dp.blackhole.supervisor.model.Stream;

public class AbnormalStageChecker extends Thread {
    private static final Log LOG = LogFactory.getLog(AbnormalStageChecker.class);

    private boolean running = true;
    private long abnormalStageCheckPeriod;
    private Supervisor supervisorService;
    private long abnormalStageDuration;
    private Map<Stage, Long> recoveryingStages; //stage --> first put time
    private Map<Stage, Long> uploadingStages;   //stage --> first put time
    
    public AbnormalStageChecker(long abnormalStageCheckPeriod, long abnormalStageDuration, long normalStageTTL) {
        this.setDaemon(true);
        this.setName("ExpiredStreamStatusChecker");
        this.abnormalStageCheckPeriod = abnormalStageCheckPeriod;
        this.abnormalStageDuration = abnormalStageDuration;
        this.supervisorService = ServiceFactory.getSupervisor();
        this.recoveryingStages = new PassiveExpiringMap<Stage, Long>(normalStageTTL);
        this.uploadingStages = new PassiveExpiringMap<Stage, Long>(normalStageTTL);
    }
    
    public void setCheckPeriod(long period) {
        this.abnormalStageCheckPeriod = period;
    }
    
    public long getCheckPeriod() {
        return abnormalStageCheckPeriod;
    }

    @Override
    public void run() {
        while (running) {
            try {
                Thread.sleep(abnormalStageCheckPeriod);
                check();
            } catch (InterruptedException e) {
                LOG.info("ExpiredStreamStatusChecker thread interrupted");
                running =false;
            }
        }
    }
    
    public void check() {
        Set<String> topics = supervisorService.getAllTopicNames();
        for (String topic : topics) {
            List<Stream> streams = supervisorService.getAllStreams(topic);
            for (Stream stream : streams) {
                List<Stage> stages = new ArrayList<Stage>(stream.getStages());
                for (Stage stage : stages) {
                    switch (stage.getStatus()) {
                    case Stage.RECOVERYING:
                        if (verifyStatusExpired(recoveryingStages, stage, abnormalStageDuration)) {
                            //trigger to re-recovery
                            List<String> agentServers = new ArrayList<String>(1);
                            agentServers.add(Util.getHostFromSource(stream.getSource()));
                            supervisorService.sendRestart(agentServers);
                        }
                        break;
                    case Stage.UPLOADING:
                        if (verifyStatusExpired(uploadingStages, stage, abnormalStageDuration)) {
                            //trigger to recovery form broker in the future, now just restart
                            List<String> agentServers = new ArrayList<String>(1);
                            agentServers.add(Util.getHostFromSource(stream.getSource()));
                            supervisorService.sendRestart(agentServers);
                        }
                        break;
                    default:
                        break;
                    }
                }
            }
        }
    }

    private boolean verifyStatusExpired(Map<Stage, Long> stages, Stage stage, long abnormalStageDuration) {
        Long firstPutTime = null;
        if ((firstPutTime = stages.get(stage)) == null) {
            stages.put(stage, Util.getTS());
        } else {
            if (Util.getTS() - firstPutTime >= abnormalStageDuration) {
                stages.remove(stage);
                return true;
            } else {
                stages.put(stage, firstPutTime);
            }
        }
        return false;
    }
}

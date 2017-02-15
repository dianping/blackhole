package com.dp.blackhole.datachecker;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.rest.ServiceFactory;
import com.dp.blackhole.supervisor.Supervisor;
import com.dp.blackhole.supervisor.model.Stream;

public class Checkpoint extends Thread {
    private static final Log LOG = LogFactory.getLog(Checkpoint.class);

    private boolean running = true;
    private File checkpiontFile;
    private long period;
    private Supervisor supervisorService;
    private StreamsStatus status;
    
    public Checkpoint(String logFilePath, long period) {
        this.setDaemon(true);
        this.setName("checkpiont");
        this.checkpiontFile = new File(logFilePath);
        this.period = period;
        this.supervisorService = ServiceFactory.getSupervisor();
        try {
            this.status = reload();
            LOG.info("CHECKPOINT reloaded.");
        } catch (IOException e) {
            this.status = new StreamsStatus();
            LOG.info("CHECKPOINT new it.");
        }
    }
    
    public void setPeriod(long period) {
        this.period = period;
    }
    
    public long getPeriod() {
        return period;
    }

    public StreamsStatus getStatus() {
        return status;
    }
    
    public void removeCheckpoint(Stream stream) {
        status.removeStream(stream);
    }

    private StreamsStatus reload() throws IOException {
        return (StreamsStatus) Util.deserialize(FileUtils.readFileToByteArray(checkpiontFile));
    }
    
    @Override
    public void run() {
        while (running) {
            try {
                Thread.sleep(period);
                checkpoint();
            } catch (InterruptedException e) {
                LOG.info("CHECKPOINT thread interrupted");
                running =false;
            }
        }
    }
    
    public void checkpoint() {
        LOG.info("CHECKPOINT begin...");
        boolean shouldPersist = false;
        Set<String> topics = supervisorService.getAllTopicNames();
        for (String topic : topics) {
            List<Stream> streams = supervisorService.getAllStreams(topic);
            for (Stream stream : streams) {
                if(status.update(stream)) {
                    shouldPersist = true;
                }
            }
        }
        if (shouldPersist) {
            LOG.info("CHECKPOINT persisting...");
            byte[] toWrite = Util.serialize(status);
            try {
                FileUtils.writeByteArrayToFile(checkpiontFile, toWrite);
            } catch (IOException e) {
                LOG.error("write checkpiont file faild", e);
            }
            LOG.info("CHECKPOINT completed!");
        } else {
            LOG.info("CHECKPOINT skipped!");
        }
    }

    public long getStoredLastSuccessTs(Stream stream) {
        return status.getLastSuccessTs(stream);
    }
}

package com.dp.blackhole.agent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AgentListener extends Thread {
    private static final Log LOG = LogFactory.getLog(AgentListener.class);
    private static final long checkTime = 2 * 1000;
    private static final String GUARD_AGENT_DIRECTORY = ".";
    private File fileAgentPID;
    private String FILE_PID_PATH = "./Agent.pid";

    private File fileAgent;
    private FileOutputStream fileOutputStreamAgent;
    private FileChannel fileChannelAgent;
    private FileLock fileLockAgent;
    private String AGENTFILELOCKPATH = "./AgentFileLock";
    private String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

    private File fileGuardAgent;
    private FileOutputStream fileOutputStreamGuardAgent;
    private FileChannel fileChannelGuardAgent;
    private FileLock fileLockGuardAgent;
    private String GUARDAGENTLOCKPATH = "./GuardAgentFileLock";

    public AgentListener() {
        fileAgentPID = new File(FILE_PID_PATH);
        fileAgent = new File(AGENTFILELOCKPATH);
        try {
            if (!fileAgentPID.exists()) {
                fileAgentPID.createNewFile();
            }

            if (!fileAgent.exists()) {
                fileAgent.createNewFile();
            }
            fileOutputStreamAgent = new FileOutputStream(fileAgent, true);
            fileChannelAgent = fileOutputStreamAgent.getChannel();
            fileLockAgent = fileChannelAgent.tryLock();
            if (fileLockAgent == null) {
                System.exit(0);
            } else {
                FileWriter fw = new FileWriter(fileAgentPID, false);
                fw.write(pid);
                fw.close();
            }
        } catch (IOException e) {
            LOG.error("file lock Agent error: " + e);
        }

        fileGuardAgent = new File(GUARDAGENTLOCKPATH);
        try {
            if (!fileGuardAgent.exists()) {
                fileAgent.createNewFile();
            }
            fileOutputStreamGuardAgent = new FileOutputStream(fileGuardAgent, true);
            fileChannelGuardAgent = fileOutputStreamGuardAgent.getChannel();
        } catch (IOException e) {
            LOG.error("file lock GuardAgent error: " + e);
        }
    }

    protected void checkFile() {
        if (!fileAgent.exists()) {
            // TODO
        }
    }

    protected boolean checkoutGuardAgent() {
        try {
            fileLockGuardAgent = fileChannelGuardAgent.tryLock();
            if (fileLockGuardAgent == null) {
                return true;
            } else {
                fileLockGuardAgent.release();
                return false;
            }
        } catch (IOException e) {
            LOG.error("error: " + fileLockGuardAgent);
            System.exit(0);
        }
        return true;
    }

    @Override
    public void run() {
        while (true) {
            if (!checkoutGuardAgent()) {
                ProcessBuilder pb = new ProcessBuilder("sh", "./guardAgent.sh");
                pb.directory(new File(GUARD_AGENT_DIRECTORY));
                try {
                    pb.start();
                } catch (IOException e) {
                    LOG.error("Guard Agent start error: " + e);
                }
            }
            try {
                Thread.sleep(checkTime);
            } catch (InterruptedException e) {
                LOG.error("sleep error" + e);
            }
        }

    }
}

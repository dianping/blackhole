package com.dp.blackhole.agent.guarder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GuardAgentListener {
    private static final Log LOG = LogFactory.getLog(GuardAgentListener.class);
    private File fileGuardAgentPID;
    private String FILE_PID_PATH = "./GuardAgent.pid";

    private File fileGuardAgent;
    private FileOutputStream fileOutputStreamGuardAgent;
    private FileChannel fileChannelGuardAgent;
    private FileLock fileLockGuardAgent;
    private String GUARDAGENT_FILE_LOCK_PATH = "./GuardAgentFileLock";
    private String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

    private File fileAgent;
    private FileOutputStream fileOutputStreamAgent;
    private FileChannel fileChannelAgent;
    private FileLock fileLockAgent;
    private String AGENTFILELOCKPATH = "./AgentFileLock";

    private File fileGuardInit;
    private FileOutputStream fileOutputStreamGuardInit;
    private FileChannel fileChannelGuardInit;
    private FileLock fileLockGuardInit;
    private String GUARDINITFILELOCKPATH = "./GuardInitFileLock";

    public GuardAgentListener() {
        fileGuardAgentPID = new File(FILE_PID_PATH);
        fileGuardAgent = new File(GUARDAGENT_FILE_LOCK_PATH);
        try {
            if (fileGuardAgentPID.exists()) {
                fileGuardAgentPID.createNewFile();
            }

            if (!fileGuardAgent.exists()) {
                fileGuardAgent.createNewFile();
            }
            fileOutputStreamGuardAgent = new FileOutputStream(fileGuardAgent, true);
            fileChannelGuardAgent = fileOutputStreamGuardAgent.getChannel();
            fileLockGuardAgent = fileChannelGuardAgent.tryLock();
            if (fileLockGuardAgent == null) {
                System.exit(0);
            } else {
                FileWriter fw = new FileWriter(fileGuardAgentPID, false);
                fw.write(pid);
                fw.close();
            }
        } catch (IOException e) {
            LOG.error("file lock GuardAgent error: " + e);
        }

        fileAgent = new File(AGENTFILELOCKPATH);
        try {
            if (!fileAgent.exists()) {
                fileAgent.createNewFile();
            }
            fileOutputStreamAgent = new FileOutputStream(fileAgent, true);
            fileChannelAgent = fileOutputStreamAgent.getChannel();
        } catch (IOException e) {
            LOG.error("file lock Agent error: " + e);
        }

        fileGuardInit = new File(GUARDINITFILELOCKPATH);
        try {
            if (!fileGuardInit.exists()) {
                fileGuardInit.createNewFile();
            }
            fileOutputStreamGuardInit = new FileOutputStream(fileGuardInit, true);
            fileChannelGuardInit = fileOutputStreamGuardInit.getChannel();
        } catch (IOException e) {
            LOG.error("file lock GuardInit error: +e");
        }

    }

    protected void checkFile() {
        if (!fileGuardAgent.exists()) {
            // TODO
        }
    }

    protected boolean checkoutAgent() {
        try {
            fileLockAgent = fileChannelAgent.tryLock();
            if (fileLockAgent == null) {
                return true;
            } else {
                fileLockAgent.release();
                return false;
            }
        } catch (IOException e) {
            LOG.error("fileLockAgent error: " + fileLockAgent);
            System.exit(0);
        }
        return true;
    }

    protected boolean checkoutGuardInit() {
        try {
            fileLockGuardInit = fileChannelGuardInit.tryLock();
            if (fileLockGuardInit == null) {
                return true;
            } else {
                fileLockGuardInit.release();
                return false;
            }
        } catch (IOException e) {
            LOG.error("fileLockGuardInit error: " + fileLockGuardInit);
            System.exit(0);
        }
        return true;
    }
}

package com.dp.blackhole.agent.guarder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GuardInitListener {
    private static final Log LOG = LogFactory.getLog(GuardInitListener.class);

    private File fileGuardInit;
    private FileOutputStream fileOutputStreamGuardInit;
    private FileChannel fileChannelGuardInit;
    private FileLock fileLockGuardInit;
    private String GUARDINIT_FILE_LOCK_PATH = "./GuardInitFileLock";

    private File fileGuardAgent;
    private FileOutputStream fileOutputStreamGuardAgent;
    private FileChannel fileChannelGuardAgent;
    protected FileLock fileLockGuardAgent;
    private String GUARDAGENT_FILE_LOCK_PATH = "./GuardAgentFileLock";

    private File fileAgent;
    private FileOutputStream fileOutputStreamAgent;
    private FileChannel fileChannelAgent;
    protected FileLock fileLockAgent;
    private String AGENT_FILE_LOCK_PATH = "./AgentFileLock";

    public GuardInitListener() {
        fileGuardInit = new File(GUARDINIT_FILE_LOCK_PATH);
        try {
            if (!fileGuardInit.exists()) {
                fileGuardInit.createNewFile();
            }
            fileOutputStreamGuardInit = new FileOutputStream(fileGuardInit, true);
            fileChannelGuardInit = fileOutputStreamGuardInit.getChannel();
            fileLockGuardInit = fileChannelGuardInit.tryLock();
            if (fileLockGuardInit == null) {
                System.exit(0);
            }
        } catch (IOException e) {
            LOG.error("file lock GuardInit error: " + e);
        }

        fileGuardAgent = new File(GUARDAGENT_FILE_LOCK_PATH);
        try {
            if (!fileGuardAgent.exists()) {
                fileGuardAgent.createNewFile();
            }
            fileOutputStreamGuardAgent = new FileOutputStream(fileGuardAgent, true);
            fileChannelGuardAgent = fileOutputStreamGuardAgent.getChannel();
        } catch (IOException e) {
            LOG.error("file lock GuardAgent error: " + e);
        }

        fileAgent = new File(AGENT_FILE_LOCK_PATH);
        try {
            if (!fileAgent.exists()) {
                fileAgent.createNewFile();
            }
            fileOutputStreamAgent = new FileOutputStream(fileAgent, true);
            fileChannelAgent = fileOutputStreamAgent.getChannel();
        } catch (IOException e) {
            LOG.error("file lock Agent error: " + e);
        }
    }

    protected void checkFile() {
        if (!fileGuardInit.exists()) {
            // TODO
        }
    }

    public boolean checkoutGuardAgent() {
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
        }
        return true;
    }

    protected void shutdown(String processName) {
        if (processName.equals("GuardAgent")) {
            shutdown(processName, "GuardAgent.pid");
        } else if (processName.equals("Agent")) {
            shutdown(processName, "Agent.pid");
        } else {
            LOG.error("process doesn't exist: " + processName);
        }
    }

    private void shutdown(String processName, String filePIDName) {
        while (true) {
            Process psGetPID;
            Process psKill;
            try {
                psGetPID = Runtime.getRuntime().exec("cat " + filePIDName);
                psGetPID.waitFor();
                BufferedReader bf = new BufferedReader(new InputStreamReader(psGetPID.getInputStream()));
                String pid = bf.readLine();
                if (pid == null) {
                    LOG.error("could not find " + processName + "'s PID");
                }
                psKill = Runtime.getRuntime().exec("kill -9 " + pid);
                System.out.println("kill" + pid);
                psKill.waitFor();
            } catch (IOException e) {
                LOG.error("IOException shutdown error " + filePIDName + e);
            } catch (InterruptedException e) {
                LOG.error("InterruptedException shutdown error " + filePIDName + e);
            }
            try {
                if (processName.equals("GuardAgent")) {
                    fileLockGuardAgent = fileChannelGuardAgent.tryLock();
                    if (fileLockGuardAgent != null) {
                        break;
                    }
                } else {
                    fileLockAgent = fileChannelAgent.tryLock();
                    if (fileLockAgent != null) {
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.error("IOException shutdown error " + filePIDName + e);
            }
        }
    }
}

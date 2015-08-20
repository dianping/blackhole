package com.dp.blackhole.agent.guarder;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GuardInit implements Runnable {
    private static final Log LOG = LogFactory.getLog(GuardInit.class);
    private static final long checkTime = 10 * 1000;
    private GuardInitListener guardInitListener = null;
    private final String GUARD_AGENT_DIRECTORY = ".";
    private final String command;

    private GuardInit(String command) {
        this.command = command;
    }

    private void startGuardAgent() {
        while (true) {
            if (!guardInitListener.checkoutGuardAgent()) {
                ProcessBuilder pb = new ProcessBuilder("sh", "./guardAgent.sh");
                pb.directory(new File(GUARD_AGENT_DIRECTORY));
                try {
                    pb.start();
                } catch (IOException e) {
                    LOG.error("GuardAgent start error: " + e);
                    break;
                }
            }
            try {
                Thread.sleep(checkTime);
            } catch (InterruptedException e) {
                LOG.error("GuardInit sleep error: " + e);
            }
        }
    }

    @Override
    public void run() {
        guardInitListener = new GuardInitListener();
        if (command.equals("guard")) {
            startGuardAgent();
        } else if (command.equals("shutdown")) {
            guardInitListener.shutdown("GuardAgent");
            guardInitListener.shutdown("Agent");
            try {
                guardInitListener.fileLockGuardAgent.release();
                guardInitListener.fileLockAgent.release();
            } catch (IOException e) {
                LOG.error("file lock release error: " + e);
            }
        } else if (command.equals("restart")) {
            guardInitListener.shutdown("GuardAgent");
            guardInitListener.shutdown("Agent");
            try {
                guardInitListener.fileLockGuardAgent.release();
                guardInitListener.fileLockAgent.release();
            } catch (IOException e) {
                LOG.error("file lock release error: " + e);
            }
            startGuardAgent();
        } else {
            LOG.error("no command found: " + command);
        }
    }

    public static void main(String[] args) {
        GuardInit guardInit;
        guardInit = new GuardInit(args[0]);
        Thread thread = new Thread(guardInit);
        thread.start();
    }
}
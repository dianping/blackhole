package com.dp.blackhole.agent.guarder;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GuardAgent {
    private static final Log LOG = LogFactory.getLog(GuardAgent.class);
    private static final long checkTime = 5 * 1000;
    private static GuardAgentListener guardAgentListener = null;
    private static final String AGENT_DIRECTORY = ".";

    public static void main(String[] args) throws Exception {
        guardAgentListener = new GuardAgentListener();
        startAgent();
    }

    private static void startAgent() throws Exception {
        while (true) {
            if (!guardAgentListener.checkoutGuardInit()) {
                // TODO
            }
            if (!guardAgentListener.checkoutAgent()) {
                ProcessBuilder pb = new ProcessBuilder("sh", "./agent.sh");
                pb.directory(new File(AGENT_DIRECTORY));
                try {
                    pb.start();
                } catch (IOException e) {
                    LOG.error("Agent start error: " + e);
                }
            }
            Thread.sleep(checkTime);
        }
    }
}

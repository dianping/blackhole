package com.dp.blackhole.rest;

import com.dp.blackhole.supervisor.ConfigManager;
import com.dp.blackhole.supervisor.Supervisor;

public class BaseResource {
    protected ConfigManager configService = ServiceFactory.getConfigManager();
    protected Supervisor supervisorService = ServiceFactory.getSupervisor();
}

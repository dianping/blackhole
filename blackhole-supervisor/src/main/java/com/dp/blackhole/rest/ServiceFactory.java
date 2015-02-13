package com.dp.blackhole.rest;

import com.dp.blackhole.supervisor.ConfigManager;
import com.dp.blackhole.supervisor.Supervisor;


public class ServiceFactory {
    
    private static ConfigManager configManager;
    private static Supervisor supervisor;
    
    public static void setConfigManger(ConfigManager _configManager) {
        configManager = _configManager;
    }
    
    public static ConfigManager getConfigManager() {
        return configManager;
    }

    public static void setSupervisor(Supervisor _supervisor) {
        supervisor = _supervisor;
    }
    
    public static Supervisor getSupervisor() {
        return supervisor;
    }
}

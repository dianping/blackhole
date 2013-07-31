package com.dp.blackhole.conf;

public final class AppConfigurationConstants {

        public static final String COMMOM_CONFIG = "conf";
        public static final String COMMOM_CONFIG_PREFIX = COMMOM_CONFIG + ".";
        public static final String APPS = "apps";
        
        public static final String PORT = "port";
        
        public static final String HOSTNAME = "hostname";
        public static final String WATCH_FILE = "watchFile";
        public static final String TRANSFER_PERIOD_VALUE = "transferPeriodValue";
        public static final String TRANSFER_PERIOD_UNIT = "transferPeriodUnit";
        public static final String FORMAT = "format";
        private AppConfigurationConstants() {
            // disable explicit object creation
        }
}

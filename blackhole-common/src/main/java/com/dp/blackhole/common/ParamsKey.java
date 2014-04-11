package com.dp.blackhole.common;

public abstract class ParamsKey {
    public static class Appconf {
        public static final String WATCH_FILE = "watchLog";
        public static final String ROLL_PERIOD = "rollPeriod";
        public static final String MAX_LINE_SIZE = "maxLineSize";
    }

    public static class LionNode {
        public final static String DEFAULT_LION_HOST = "http://lionapi.dp:8080/";
        public final static String LION_SET_PATH = "setconfig";
        public final static String LION_GET_PATH = "getconfig";
        public final static String LION_PROJECT = "blackhole";
        public final static String APPS = "blackhole.apps";
        public final static String APP_HOSTS_PREFIX = "blackhole.hosts.";
        public final static String APP_CONF_PREFIX = "blackhole.conf.";
        public final static String APP_CMDB_PREFIX = "blackhole.cmdb.";
    }

    public static class Stat {
        public static final String LINE_COUNT_TYPE = "BHLineStat";
    }

    public static class ZNode {
        public final static String ROOT = "/blackhole";
        public final static String STREAMS = "/blackhole/streams";
    }
}

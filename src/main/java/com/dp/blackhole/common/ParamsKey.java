package com.dp.blackhole.common;

public abstract class ParamsKey {
    public static class Appconf {
        public static final String WATCH_FILE = "WATCH_FILE";
        public static final String ROLL_PERIOD = "ROLL_PERIOD";
        public static final String BUFFER_SIZE = "BUFFER_SIZE";
    }
    
    public static class ZKServer {
        public final static String ZK_HOST_LIST = "ZK_HOST_LIST";
        public final static String ZK_TIMEOUT = "ZK_TIMEOUT";
    }
    
    public static class ZNode {
        public final static String ROOT = "/blackhole";
        public final static String STREAMS = "/blackhole/streams";
    }
}

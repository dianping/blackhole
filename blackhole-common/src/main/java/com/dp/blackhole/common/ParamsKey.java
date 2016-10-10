package com.dp.blackhole.common;

public abstract class ParamsKey {
    public static class TopicConf {
        public static final String APP = "app";
        public static final String WATCH_FILE = "watchLog";
        public static final String ROTATE_PERIOD = "rotatePeriod";
        public static final String ROLL_PERIOD = "rollPeriod";
        public static final String MAX_LINE_SIZE = "maxLineSize";
        public static final String PERSISTENT = "isPersist";
        public static final String READ_INTERVAL = "readInterval";
        public static final String OWNER = "owner";
        public static final String COMPRESSION = "compression";
        public static final String UPLOAD_PERIOD = "uploadPeriod";
        public static final String MINIMUM_MESSAGES_SENT = "minMsgSent";
        public static final String MESSAGE_BUFFER_SIZE = "msgBufSize";
        public static final String BANDWIDTH_PER_SEC = "bandwidthPerSec";
        public static final String PARTITION_FACTOR = "partitionFactor";
        public static final String TAIL_POSITION = "tailPosition";
        
        public static final Long FILE_TAIL = -1L;
        public static final Long FILE_HEAD = 0L;
        public static int DEFAULT_REPLICA_NUM = 2;
        public static int DEFAULT_MESSAGE_QUEUE_MIN_ACK = 1;
        public static int DEFAULT_REPLICA_FETCHSIZE = 1024 * 1024;
    }

    public static class LionNode {
        public static final String DEFAULT_LION_HOST = "http://lionapi.dp:8080/";
        public static final String LION_SET_PATH = "setconfig";
        public static final String LION_GET_PATH = "getconfig";
        public static final String LION_PROJECT = "blackhole";
        public static final String TOPIC = "blackhole.apps";
        public static final String HOSTS_PREFIX = "blackhole.hosts.";
        public static final String CONF_PREFIX = "blackhole.conf.";
        public static final String BLACKLIST = "blackhole.check.blacklist";
        public static final String BROKER_ASSIGN_LIMIT_MIN = "blackhole.broker-assign.limit.min";
        public static final String PERASSIGNMENT = "blackhole.broker.preassignment";
        public static final String OP_SCALEOUT = "+";
        public static final String OP_SCALEIN = "-";
    }

    public static class ZNode {
        public static final String ROOT = "/blackhole";
        public static final String STREAMS = "/blackhole/streams";
    }
    
    public static class HTTP {
        public static final String WILDCARD_ACL_VALUE = "*";
        public static final int INITIAL_CAPACITY = 256;
    }
    
    public static final String COMPRESSION_LZO = "lzo";
    public static final String COMPRESSION_GZ = "gz";
    public static final String LZO_INDEX_SUFFIX = ".index";
    public static final String LZO_TMP_INDEX_SUFFIX = ".index.tmp";
    public static final String COMPRESSION_NONE = "none";
    public static final String COMPRESSION_UNDEFINED = "undefined";
    public static final long DEFAULT_CLOCK_SYNC_BUF_MILLIS = 20000;
    
    public static final short SOCKET_HEARTBEAT = 0xea;
    
    public static final String TOPIC_KEY = "topic";
    public static final String PARTITION_KEY = "partition";
    public static final String BROKER_KEY = "broker";
    public static final String DEFAULT_VERSION = "UNSPCIFIED";
}

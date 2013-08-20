package com.dp.blackhole.supervisor;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.gen.ConfResPB.ConfRes.AppConfRes;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.conf.Context;

public class TestZKHelper {
    private static String connectString = "10.1.77.88:2181,10.1.77.89:2181,10.1.77.90:2181";
    private final String MAGIC = "dec8e543";
    private static ZKHelper helper = ZKHelper.getInstance();
    private static Stream stream = new Stream();
    private static Stage stage = new Stage();
    private static final String STREAM_DATA_EXPECTEDexpected = "app=testapp" + "\n" +
            "apphost=testapphost" + "\n" + 
            "period=3600" + "\n" +
            "startTs=1370100000" + "\n" + 
            "lastSuccessTs=1370200000";
    private static final String STAGE_DATA_EXPECTED = "app=testapp" + "\n" +
            "apphost=testapphost" + "\n" + 
            "collectorhost=testcollectorhost" + "\n" +
            "cleanstart=false" + "\n" + 
            "status=2" + "\n" + 
            "rollTs=1370200000" + "\n" + 
            "isCurrent=true";
    static {
        stream.app = "testapp";
        stream.appHost = "testapphost";
        stream.period = 3600l;
        stream.startTs = 1370100000;
        stream.setlastSuccessTs(1370200000l);
        
        stage.app = "testapp";
        stage.apphost = "testapphost";
        stage.collectorhost = "testcollectorhost";
        stage.rollTs = 1370200000;
        stage.cleanstart = false;
        stage.isCurrent = true;
        stage.status = 2;
    }
    
    static class MockSupervisor extends Supervisor{
        @Override
        public void emitConfToAll() {
            List<AppConfRes> appConfResList = new ArrayList<AppConfRes>();
            for (String host : helper.appHostToAppNames.keySet()) {
                List<String> appNames = helper.appHostToAppNames.get(host);
                for (String appName : appNames) {
                    Context context = ConfigKeeper.configMap.get(appName);
                    String watchFile = context.getString(ParamsKey.Appconf.WATCH_FILE);
                    String period = context.getString(ParamsKey.Appconf.ROLL_PERIOD);
                    AppConfRes appConfRes = PBwrap.wrapAppConfRes(appName, watchFile, period);
                    appConfResList.add(appConfRes);
                }
                Message message = PBwrap.wrapConfRes(appConfResList);
            }
            System.out.println(appConfResList);
        }
    }
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        helper.createConnection(new MockSupervisor(), connectString, 40000);
        helper.rawAddZNode(ParamsKey.ZNode.ROOT, "");
        helper.rawAddZNode(ParamsKey.ZNode.STREAMS, "");
        helper.rawAddZNode(ParamsKey.ZNode.CONFS, "");
//        helper.addStreamIDNode(stream);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
//        helper.delStreamIDNode(stream);
        helper.delZNode(ParamsKey.ZNode.CONFS);
        helper.delZNode(ParamsKey.ZNode.STREAMS);
        helper.delZNode(ParamsKey.ZNode.ROOT);
        helper.releaseConnection();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetValueByKey() {
        String key = "app=testapp";
        assertEquals("testapp", helper.getValue(key));
        key = "app=testapp";
        assertEquals("testapp", helper.getValue(key));
        key = "appHost=testapphost";
        assertEquals("testapphost", helper.getValue(key));
        key = "period=3600";
        assertEquals("3600", helper.getValue(key));
    }
    
    @Test
    public void testLoadAllConfs() throws KeeperException, InterruptedException, IOException {
        ConfigKeeper.configMap.clear();
        StringBuffer data = new StringBuffer();
        StringBuffer data2 = new StringBuffer();
        data.append("\n").append("APP_HOSTS = host1;host2; ").append("\n").append("WATCH_FILE = /tmp/1.log").append("\n").append("ROLL_PERIOD = 60").append("\n").append("\n");
        data2.append("\n").append("APP_HOSTS = host3;host1; ").append("\n").append("WATCH_FILE = /tmp/1.log").append("\n").append("ROLL_PERIOD = 60").append("\n").append("\n");
        helper.rawAddZNode(ParamsKey.ZNode.CONFS + "/" + MAGIC, data.toString());
        helper.rawAddZNode(ParamsKey.ZNode.CONFS + "/" + MAGIC + MAGIC, data2.toString());
        helper.loadAllConfs();
        assertEquals("/tmp/1.log", ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.Appconf.WATCH_FILE));
        assertEquals("60", ConfigKeeper.configMap.get(MAGIC).getString(ParamsKey.Appconf.ROLL_PERIOD));
        assertEquals(2, helper.appHostToAppNames.get("host1").size());
        assertEquals(1, helper.appHostToAppNames.get("host2").size());
        assertEquals(1, helper.appHostToAppNames.get("host3").size());
        helper.delZNode(ParamsKey.ZNode.CONFS + "/" + MAGIC);
        helper.delZNode(ParamsKey.ZNode.CONFS + "/" + MAGIC + MAGIC);
    }
    
    @Test
    public void testProecss() throws KeeperException, InterruptedException {
        helper.leftChildrenWatcher(ParamsKey.ZNode.CONFS, null);
        if (helper.appHostToAppNames == null) {
            helper.appHostToAppNames = new ConcurrentHashMap<String, ArrayList<String>>();
        }
        if (helper.appSet == null) {
            helper.appSet = new HashSet<String>();
        }
        StringBuffer data = new StringBuffer();
        data.append("\n").append("APP_HOSTS = host4;host5; ").append("\n").append("WATCH_FILE = /tmp/1.log").append("\n").append("ROLL_PERIOD = 60").append("\n").append("\n");
        helper.rawAddZNode(ParamsKey.ZNode.CONFS + "/" + MAGIC + MAGIC + MAGIC, data.toString());
        Thread.sleep(1000); //can not be remove
        assertEquals("/tmp/1.log", ConfigKeeper.configMap.get(MAGIC + MAGIC + MAGIC).getString(ParamsKey.Appconf.WATCH_FILE));
        assertEquals("60", ConfigKeeper.configMap.get(MAGIC + MAGIC + MAGIC).getString(ParamsKey.Appconf.ROLL_PERIOD));
        assertEquals(1, helper.appHostToAppNames.get("host4").size());
        assertEquals(1, helper.appHostToAppNames.get("host5").size());
        helper.delZNode(ParamsKey.ZNode.CONFS + "/" + MAGIC + MAGIC + MAGIC);
    }
}

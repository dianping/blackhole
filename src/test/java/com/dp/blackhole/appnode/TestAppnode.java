package com.dp.blackhole.appnode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.appnode.Appnode;
import com.dp.blackhole.common.PBwrap;
import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.gen.ConfResPB.ConfRes.AppConfRes;
import com.dp.blackhole.common.gen.MessagePB.Message;
import com.dp.blackhole.conf.ConfigKeeper;
import com.dp.blackhole.simutil.Util;
public class TestAppnode {
    private static final String MAGIC = "9vjrder3";
    private static Appnode appnode;
    private static String client;
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        appnode = new Appnode("localhost");
        ConfigKeeper conf = new ConfigKeeper();
        conf.addRawProperty(MAGIC+".WATCH_FILE", "/tmp/" + MAGIC + ".log");
        appnode.fillUpAppLogsFromConfig();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testAssignCollectorProcess() {
        Message bad = getMessageOfAssignCollector(MAGIC + MAGIC);
        assertFalse(appnode.process(bad));
        Message good = getMessageOfAssignCollector(MAGIC);
        assertTrue(appnode.process(good));
    }
    
    @Test
    public void testRecoveryRollProcess() {
        Message bad = getMessageOfRecoveryRoll(MAGIC + MAGIC);
        assertFalse(appnode.process(bad));
        Message good = getMessageOfRecoveryRoll(MAGIC);
        assertTrue(appnode.process(good));
    }
    
    @Test
    public void testConfResProcess() throws IOException {
        ConfigKeeper.configMap.clear();
        File file1 = new File("/tmp/testApp1.log");
        File file2 = new File("/tmp/testApp2.log");
        file1.createNewFile();
        file2.createNewFile();
        AppConfRes appConfRes1 = getAppConfRes(MAGIC, "/tmp/testApp1.log", "60");
        AppConfRes appConfRes2 = getAppConfRes(MAGIC+MAGIC, "/tmp/testApp2.log", "600");
        List<AppConfRes> appConfResList = new ArrayList<AppConfRes>();
        appConfResList.add(appConfRes1);
        appConfResList.add(appConfRes2);
        Message message = getMessageOfConfRes(appConfResList);
        assertTrue(appnode.process(message));
        ConfigKeeper.configMap.clear();
        file1.delete();
        file2.delete();
        assertFalse(appnode.process(message));
    }
    
    @Test
    public void testUnknowMessageProcess() {
        Message unknow = getUnknowMessage();
        assertFalse(appnode.process(unknow));
    }

    private Message getMessageOfAssignCollector(String appName) {
        return PBwrap.wrapAssignCollector(appName, Util.HOSTNAME);
    }
    
    private Message getMessageOfRecoveryRoll(String appName) {
        return PBwrap.wrapRecoveryRoll(appName, Util.HOSTNAME, Util.rollTS);
    }

    private AppConfRes getAppConfRes(String appName, String watchFile, String period) {
        return PBwrap.wrapAppConfRes(appName, watchFile, period);
    }
    
    private Message getMessageOfConfRes(List<AppConfRes> appConfResList) {
        return PBwrap.wrapConfRes(appConfResList);
    }
    
    private Message getUnknowMessage() {
        return PBwrap.wrapReadyCollector(MAGIC, Util.HOSTNAME, 3600l, Util.HOSTNAME, 1l);
    }
    
    @Test
    public void testLoadLocalConfig() throws ParseException, IOException {
        File confFile = File.createTempFile("app.conf", null);
        OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(confFile, true));
        writer.write("testApp.WATCH_FILE = /tmp/testApp.log\n");
        writer.write("testApp.second = sencond preperty\n");
        writer.flush();
        writer.close();
        
        String[] args = new String[2];
        args[0] = "-f";
        args[1] = confFile.getAbsolutePath();
        Appnode appnode = new Appnode(client);
        appnode.setArgs(args);
        assertTrue(appnode.loadAppConfFromLocal());
        assertTrue(ConfigKeeper.configMap.containsKey("testApp"));
        String path = ConfigKeeper.configMap.get("testApp")
                .getString(ParamsKey.Appconf.WATCH_FILE);
        assertEquals(path, "/tmp/testApp.log");
        assertTrue(ConfigKeeper.configMap.containsKey("testApp"));
        String second = ConfigKeeper.configMap.get("testApp")
                .getString("second");
        assertEquals(second, "sencond preperty");
        confFile.delete();
    }
}

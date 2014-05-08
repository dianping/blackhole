package com.dp.blackhole.broker;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dp.blackhole.broker.HDFSMarker;

public class TestHDFSMarker {
    private final String MAGIC = "c344g53";
    private FileSystem fs;
    private int port = 40005;

    @Before
    public void setUp() throws Exception {
        try {
            fs = (new Path("/tmp")).getFileSystem(new Configuration());
        } catch (IOException e) {
            throw e;
        }
    }

    @After
    public void tearDown() throws Exception {
        fs.delete(new Path(SimBroker.SCHEMA + SimBroker.BASE_PATH), true);
    }

    @Test
    public void testMark() throws IOException, InterruptedException {
        new SimBroker(port);
        HDFSMarker marker = new HDFSMarker(SimBroker.getRollMgr(), fs, SimBroker.getRollIdent(MAGIC));
        SimBroker.getRollMgr().init("/tmp/hdfs", ".gz", port, 5000, 1, 1, 60000);
        Thread thread = new Thread(marker);
        thread.start();
        thread.join();
        Path exceptedMarkFile = new Path(SimBroker.SCHEMA + SimBroker.BASE_PATH 
                + MAGIC + "/2013-01-01/15/" + "_" + SimBroker.HOSTNAME + "@"
                + MAGIC + "_2013-01-01.15");
        assertTrue(fs.exists(exceptedMarkFile));
    }
}

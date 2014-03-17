package com.dp.blackhole.collectornode;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHDFSMarker {
    private final String MAGIC = "c344g53";
    private FileSystem fs;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw e;
        }
    }

    @After
    public void tearDown() throws Exception {
        fs.delete(new Path(SimCollectornode.SCHEMA + SimCollectornode.BASE_PATH), true);
    }

    @Test
    public void testMark() throws IOException, InterruptedException {
        HDFSMarker marker = new HDFSMarker(SimCollectornode.getSimpleInstance("mark", MAGIC, fs),
                fs, SimCollectornode.getRollIdent(MAGIC));
        Thread thread = new Thread(marker);
        thread.start();
        thread.join();
        Path exceptedMarkFile = new Path(SimCollectornode.SCHEMA + SimCollectornode.BASE_PATH 
                + MAGIC + "/2013-01-01/15/" + "_" + SimCollectornode.HOSTNAME + "@"
                + MAGIC + "_2013-01-01.15");
        assertTrue(fs.exists(exceptedMarkFile));
    }
}

package com.dp.blackhole.agent;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.dp.blackhole.agent.TopicMeta;
import com.dp.blackhole.agent.LogReader;
import com.dp.blackhole.agent.TopicMeta.TopicId;
import com.dp.blackhole.agent.persist.LocalRecorder;
import com.dp.blackhole.agent.persist.Record;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"org.apache.hadoop.*", "com.sun.*", "net.contentobjects.*"})
@PrepareForTest(LocalRecorder.class)
public class TestLogReader2 {
    private static final String MAGIC = "adfadfecw";

    @Test
    public void testRestoreMissingRotationRecords() {
        TopicId topicId = new TopicId(MAGIC, null);
        TopicMeta meta = new TopicMeta(topicId, SimAgent.TEST_ROLL_FILE, 3600, 3600, 1024, 1L, 5, 4096);
        LogReader spyReader = spy(new LogReader(new SimAgent(), meta, "/tmp/test1"));
        LocalRecorder spyLocalRecorder = spy(new LocalRecorder("/tmp/test1", meta));
        try {
            doNothing().when(spyLocalRecorder, "persist");
            when(spyReader.getRecoder()).thenReturn(spyLocalRecorder);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long rollTs1 = 1421036400000L; //2015-01-12 12:20:00
        spyLocalRecorder.record(Record.ROLL, rollTs1, 100);
        long rollTs2 = 1421036700000L; //2015-01-12 12:25:00
        spyLocalRecorder.record(Record.ROLL, rollTs2, 200);
        long resumeRollTs = 1421065320000L; //2015-01-12 20:22:00
        spyReader.restoreMissingRotationRecords(300, 3600, resumeRollTs);
//        assertEquals(10, spyLocalRecorder.getSnapshot().getRecords().size());
        assertEquals(1421036400000L, spyLocalRecorder.getSnapshot().getRecords().get(0).getRollTs());
        assertEquals(1421036700000L, spyLocalRecorder.getSnapshot().getRecords().get(1).getRollTs());
        assertEquals(1421038500000L, spyLocalRecorder.getSnapshot().getRecords().get(2).getRollTs());
        assertEquals(1421042100000L, spyLocalRecorder.getSnapshot().getRecords().get(3).getRollTs());
        assertEquals(1421045700000L, spyLocalRecorder.getSnapshot().getRecords().get(4).getRollTs());
        assertEquals(1421049300000L, spyLocalRecorder.getSnapshot().getRecords().get(5).getRollTs());
        assertEquals(1421052900000L, spyLocalRecorder.getSnapshot().getRecords().get(6).getRollTs());
        assertEquals(1421056500000L, spyLocalRecorder.getSnapshot().getRecords().get(7).getRollTs());
        assertEquals(1421060100000L, spyLocalRecorder.getSnapshot().getRecords().get(8).getRollTs());
        assertEquals(1421063700000L, spyLocalRecorder.getSnapshot().getRecords().get(9).getRollTs());
    }
}

package com.dp.blackhole.agent;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.dp.blackhole.agent.LogReader;
import com.dp.blackhole.agent.AgentMeta.TopicId;
import com.dp.blackhole.agent.persist.LocalRecorder;
import com.dp.blackhole.agent.persist.Record;
import com.dp.blackhole.common.Util;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"org.apache.hadoop.*", "com.sun.*", "net.contentobjects.*"})
@PrepareForTest(LocalRecorder.class)
public class TestLocalRecorder {
    private static final String MAGIC = "adfadfecw";
    private TopicId topicId;
    private AgentMeta meta;
    private LogReader spyReader;
    private LocalRecorder spyLocalRecorder;
    private long PERIOD_DAY = 86400L;
    private long PERIOD_HOUR = 3600L;
    private long PERIOD_ROLL = 300L;
    
    private void init(long rotatePeriod, long rollPeriod) {
        topicId = new TopicId(MAGIC, null);
        meta = new AgentMeta(topicId, SimAgent.TEST_ROLL_FILE, rotatePeriod, rollPeriod, 1024, 1L, 5, 4096, 1024*1024, 1);
        spyReader = spy(new LogReader(new SimAgent(), meta, "/tmp/test1"));
        spyLocalRecorder = spy(new LocalRecorder("/tmp/test1", meta));
        when(spyReader.getRecoder()).thenReturn(spyLocalRecorder);
        try {
            doNothing().when(spyLocalRecorder, "persist");
        } catch (Exception e) {
            e.printStackTrace();
        }
        spyLocalRecorder.getSnapshot().clear();
    }
    
    @Test
    public void testRestoreMissingRotationRecords() {
        init(PERIOD_HOUR, PERIOD_ROLL);
        long rollTs1 = 1421036400000L; //2015-01-12 12:20:00
        spyReader.record(Record.ROLL, rollTs1, 100);
        long rollTs2 = 1421036700000L; //2015-01-12 12:25:00
        spyReader.record(Record.ROLL, rollTs2, 200);
        long resumeRollTs = 1421065320000L; //2015-01-12 20:22:00
        spyReader.restoreMissingRotationRecords(PERIOD_ROLL, PERIOD_HOUR, resumeRollTs);
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


    
    @Test
    public void testRollHourlyRotateHourly() {
        init(PERIOD_HOUR, PERIOD_HOUR);
        long ROTATE_24_16 = 1440403200000L;     //2015-08-24 16:00:00
        long ROTATE_24_17 = 1440406800000L;
        long ROTATE_24_18 = 1440410400000L;
        long RESUME_24_18_52 = 1440413520000L;  //2015-08-24 18:52:00
        long ROTATE_24_19 = 1440414000000L;
        long RESUME_25_01_20 = 1440436800000L;  //2015-08-25 01:20:00
        long ROTATE_25_01 = 1440435600000L;

        //there are two normal records
        spyLocalRecorder.record(Record.ROTATE, ROTATE_24_16, 0, -1, ROTATE_24_16);
        spyReader.record(Record.ROTATE, ROTATE_24_17, 100);
        
        
        //crash at 2015-08-24 18:12:00 and resume at 2015-08-24 18:52
        spyReader.restoreMissingRotationRecords(PERIOD_HOUR, PERIOD_HOUR, RESUME_24_18_52);
        spyReader.record(Record.ROTATE, ROTATE_24_18, 100);
        
        //crash at 2015-08-24 19:20:00 and resume at 2015-08-25 01:20
        spyReader.restoreMissingRotationRecords(PERIOD_HOUR, PERIOD_HOUR, RESUME_25_01_20);
        spyReader.record(Record.ROTATE, ROTATE_25_01, 100);
        
        assertEquals(ROTATE_24_16, spyLocalRecorder.getSnapshot().getRecords().get(0).getRollTs());
        assertEquals(ROTATE_24_17, spyLocalRecorder.getSnapshot().getRecords().get(1).getRollTs());
        assertEquals(ROTATE_24_18, spyLocalRecorder.getSnapshot().getRecords().get(2).getRollTs());
        assertEquals(ROTATE_24_19, spyLocalRecorder.getSnapshot().getRecords().get(3).getRollTs());
        assertEquals(ROTATE_25_01, spyLocalRecorder.getSnapshot().getRecords().get(9).getRollTs());
    }
    
    @Test
    public void testRollEveryFileMinutesRotateHourly() {
        init(PERIOD_HOUR, PERIOD_ROLL);
        long ROTATE_24_17 = 1440406800000L;
        long ROTATE_24_16_55 = 1440406500000L;
        long ROTATE_24_17_55 = 1440410100000L;
        long ROLL_24_18_00 = 1440410400000L;    //2015-08-24 18:00:00
        long ROLL_24_18_05 = 1440410700000L;    //2015-08-24 18:05:00
        long ROLL_24_18_50 = 1440413400000L;
        long ROTATE_24_18_55 = 1440413700000L;
        long ROLL_24_19_00 = 1440414000000L;
        long ROLL_24_19_05 = 1440414300000L;
        long RESUME_24_18_52 = 1440413520000L;  //2015-08-24 18:50:00
        long RESUME_25_01_20 = 1440436800000L;  //2015-08-25 01:20:00
        long ROTATE_24_19_55 = 1440417300000L;
        long ROTATE_24_23_55 = 1440431700000L;
        long ROTATE_25_00_55 = 1440435300000L;
        
        //there are several normal records
        spyLocalRecorder.record(Record.ROTATE, ROTATE_24_16_55, 0, -1, ROTATE_24_17);
        spyReader.record(Record.ROTATE, ROTATE_24_17_55, 100);
        spyReader.record(Record.ROLL, ROLL_24_18_00, 100);
        spyReader.record(Record.ROLL, ROLL_24_18_05, 200);
        
        //crash at 2015-08-24 18:12:00 and resume at 2015-08-24 18:52
        spyReader.restoreMissingRotationRecords(PERIOD_ROLL, PERIOD_HOUR, RESUME_24_18_52);
        
        //there are several normal records again
        spyReader.record(Record.ROLL, ROLL_24_18_50, 900);
        spyReader.record(Record.ROTATE, ROTATE_24_18_55, 1000);
        spyReader.record(Record.ROLL, ROLL_24_19_00, 100);
        spyReader.record(Record.ROLL, ROLL_24_19_05, 200);
        
        //crash at 2015-08-24 19:12:00 and resume at 2015-08-25 01:20
        spyReader.restoreMissingRotationRecords(PERIOD_ROLL, PERIOD_HOUR, RESUME_25_01_20);

        assertEquals(ROTATE_24_16_55, spyLocalRecorder.getSnapshot().getRecords().get(0).getRollTs());
        assertEquals(ROTATE_24_17_55, spyLocalRecorder.getSnapshot().getRecords().get(1).getRollTs());
        assertEquals(ROLL_24_18_00, spyLocalRecorder.getSnapshot().getRecords().get(2).getRollTs());
        assertEquals(ROLL_24_18_05, spyLocalRecorder.getSnapshot().getRecords().get(3).getRollTs());
        assertEquals(ROLL_24_18_50, spyLocalRecorder.getSnapshot().getRecords().get(4).getRollTs());
        assertEquals(ROTATE_24_18_55, spyLocalRecorder.getSnapshot().getRecords().get(5).getRollTs());
        assertEquals(ROLL_24_19_00, spyLocalRecorder.getSnapshot().getRecords().get(6).getRollTs());
        assertEquals(ROLL_24_19_05, spyLocalRecorder.getSnapshot().getRecords().get(7).getRollTs());
        assertEquals(ROTATE_24_19_55, spyLocalRecorder.getSnapshot().getRecords().get(8).getRollTs());
        assertEquals(ROTATE_24_23_55, spyLocalRecorder.getSnapshot().getRecords().get(12).getRollTs());
        assertEquals(ROTATE_25_00_55, spyLocalRecorder.getSnapshot().getRecords().get(13).getRollTs());
    }
    
    @Test
    public void testRollDaylyRotateDayly() {
        init(PERIOD_DAY, PERIOD_DAY);
        long ROTATE_23 = 1440259200000L;
        long ROTATE_24 = 1440345600000L;
        long RESUME_24_18_52 = 1440413520000L;  //2015-08-24 18:52:00
        long ROTATE_25 = 1440432000000L;
        long ROTATE_26 = 1440518400000L;
        long RESUME_27_01_20 = 1440609600000L;  //2015-08-27 01:20:00
        long ROTATE_27 = 1440604800000L;
        
        //there are two normal records
        spyLocalRecorder.record(Record.ROTATE, ROTATE_23, 0, -1, ROTATE_23);
        
        //crash at 2015-08-24 18:12:00 and resume at 2015-08-24 18:52
        spyReader.restoreMissingRotationRecords(PERIOD_DAY, PERIOD_DAY, RESUME_24_18_52);
        spyReader.record(Record.ROTATE, ROTATE_24, 24000);
        
        //crash at 2015-08-24 19:12:00 and resume at 2015-08-27 01:20
        spyReader.restoreMissingRotationRecords(PERIOD_DAY, PERIOD_DAY, RESUME_27_01_20);
        spyReader.record(Record.ROTATE, ROTATE_27, 24000);

        assertEquals(ROTATE_23, spyLocalRecorder.getSnapshot().getRecords().get(0).getRollTs());
        assertEquals(ROTATE_24, spyLocalRecorder.getSnapshot().getRecords().get(1).getRollTs());
        assertEquals(ROTATE_25, spyLocalRecorder.getSnapshot().getRecords().get(2).getRollTs());
        assertEquals(ROTATE_26, spyLocalRecorder.getSnapshot().getRecords().get(3).getRollTs());
        assertEquals(ROTATE_27, spyLocalRecorder.getSnapshot().getRecords().get(4).getRollTs());
    }
    
    @Test
    public void testRollHourlyRotateDayly() {
        init(PERIOD_DAY, PERIOD_HOUR);
        long ROTATE_24 = 1440345600000L;
        long ROLL_24_16 = 1440403200000L;     //2015-08-24 16:00:00
        long ROLL_24_17 = 1440406800000L;
        long ROLL_24_18 = 1440410400000L;
        long RESUME_24_18_52 = 1440413520000L;  //2015-08-24 18:52:00
        long ROTATE_24_23 = 1440428400000L;
        long RESUME_25_01_20 = 1440436800000L;  //2015-08-25 01:20:00
        long ROLL_25_01 = 1440435600000L;
        //there are two normal records
        spyLocalRecorder.record(Record.ROLL, ROLL_24_16, 16001, 17000, ROTATE_24);
        spyReader.record(Record.ROLL, ROLL_24_17, 18000);
        
        //crash at 2015-08-24 18:12:00 and resume at 2015-08-24 18:52
        spyReader.restoreMissingRotationRecords(PERIOD_HOUR, PERIOD_DAY, RESUME_24_18_52);
        spyReader.record(Record.ROLL, ROLL_24_18, 19000);
        
        //crash at 2015-08-24 19:20:00 and resume at 2015-08-25 01:20
        spyReader.restoreMissingRotationRecords(PERIOD_HOUR, PERIOD_DAY, RESUME_25_01_20);
        spyReader.record(Record.ROLL, ROLL_25_01, 2000);

        assertEquals(ROLL_24_16, spyLocalRecorder.getSnapshot().getRecords().get(0).getRollTs());
        assertEquals(ROLL_24_17, spyLocalRecorder.getSnapshot().getRecords().get(1).getRollTs());
        assertEquals(ROLL_24_18, spyLocalRecorder.getSnapshot().getRecords().get(2).getRollTs());
        assertEquals(ROTATE_24_23, spyLocalRecorder.getSnapshot().getRecords().get(3).getRollTs());
        assertEquals(ROLL_25_01, spyLocalRecorder.getSnapshot().getRecords().get(4).getRollTs());
    }
    
    
    @Test
    public void testComputeStartOffset() {
        long END_OF_ROLL = 1000;
        long END_OF_ROTATE = 10000;
        long OUT_OVER_EOF = 10001;
        long START_OF_NEXT_ROLL = 1001;
        long START_OF_NEW_ROTATE = 0;
        long rollTs;
        long _22D = 1440172800000L;
        long _23D = 1440259200000L;
        long _24D = 1440345600000L;
        long _25D = 1440432000000L;
        long _26D = 1440518400000L;
        long _24D16H = 1440403200000L;  //2015-08-24 16:00:00
        long _24D17H = 1440406800000L;
        long _24D18H = 1440410400000L;
        long _24D19H = 1440414000000L;
        long _24D23H = 1440428400000L;
        long _26D17H = 1440579600000L;
        long _26D18H = 1440583200000L;
        long _24D17H55M = 1440410100000L;
        long _24D18H50M = 1440413400000L;
        long _24D18H55M = 1440413700000L;
        long _26D17H25M = 1440581100000L;
        long _26D17H30M = 1440581400000L;
        
        init(PERIOD_HOUR, PERIOD_HOUR);
        
        //simulate rotation happen at 2015-08-24 17:00:00
        spyLocalRecorder.record(Record.ROTATE, _24D16H, 0, END_OF_ROTATE, _24D16H);
        long currentRotation = setCurrentRotation(_24D17H, PERIOD_HOUR);
        assertEquals(_24D17H, currentRotation);
        
        //evaluate rotation happen at 2015-08-24 18:00:00
        long rotateTsAssociateWithRollPeriod = Util.getLatestRollTsUnderTimeBuf(_24D18H, PERIOD_HOUR, 0);
        assertEquals(_24D17H, rotateTsAssociateWithRollPeriod);
        assertEquals(START_OF_NEW_ROTATE, spyReader.computeStartOffset(rotateTsAssociateWithRollPeriod, PERIOD_HOUR));
        setCurrentRotation(_24D18H, PERIOD_HOUR);
        
        rotateTsAssociateWithRollPeriod = Util.getLatestRollTsUnderTimeBuf(_26D18H, PERIOD_HOUR, 0);
        assertEquals(_26D17H, rotateTsAssociateWithRollPeriod);
        assertEquals(OUT_OVER_EOF, spyReader.computeStartOffset(rotateTsAssociateWithRollPeriod, PERIOD_HOUR));
        setCurrentRotation(_26D18H, PERIOD_HOUR);
        
        init(PERIOD_HOUR, PERIOD_ROLL);
        
        spyLocalRecorder.record(Record.ROTATE, _24D17H55M, 0, END_OF_ROTATE, _24D17H);
        currentRotation = setCurrentRotation(_24D18H, PERIOD_HOUR);
        //some rolls passed but before rotation at 19:00
        spyLocalRecorder.record(Record.ROLL, _24D18H50M, 0, END_OF_ROLL, currentRotation);
        
        rotateTsAssociateWithRollPeriod = Util.getLatestRollTsUnderTimeBuf(_24D19H, PERIOD_ROLL, 0);
        assertEquals(_24D18H55M, rotateTsAssociateWithRollPeriod);
        assertEquals(START_OF_NEXT_ROLL, spyReader.computeStartOffset(rotateTsAssociateWithRollPeriod, PERIOD_HOUR));
        setCurrentRotation(_24D19H, PERIOD_HOUR);
        
        //evaluate roll at 2015-08-26 17:30:00 if watch file not increased
        rollTs = Util.getLatestRollTsUnderTimeBuf(_26D17H30M, PERIOD_ROLL, 0);
        assertEquals(_26D17H25M, rollTs);
        assertEquals(START_OF_NEXT_ROLL, spyReader.computeStartOffset(rollTs, PERIOD_HOUR));
        
        setCurrentRotation(_26D17H, PERIOD_HOUR);
        assertEquals(START_OF_NEW_ROTATE, spyReader.computeStartOffset(rollTs, PERIOD_HOUR));

        init(PERIOD_DAY, PERIOD_DAY);
        spyLocalRecorder.record(Record.ROTATE, _22D, 0, END_OF_ROTATE, _22D);
        setCurrentRotation(_23D, PERIOD_DAY);
        
        rotateTsAssociateWithRollPeriod = Util.getLatestRollTsUnderTimeBuf(_24D, PERIOD_DAY, 0);
        assertEquals(_23D, rotateTsAssociateWithRollPeriod);
        assertEquals(START_OF_NEW_ROTATE, spyReader.computeStartOffset(rotateTsAssociateWithRollPeriod, PERIOD_DAY));
        setCurrentRotation(_24D, PERIOD_DAY);
        
        rotateTsAssociateWithRollPeriod = Util.getLatestRollTsUnderTimeBuf(_26D, PERIOD_DAY, 0);
        assertEquals(_25D, rotateTsAssociateWithRollPeriod);
        assertEquals(OUT_OVER_EOF, spyReader.computeStartOffset(rotateTsAssociateWithRollPeriod, PERIOD_DAY));
        setCurrentRotation(_26D, PERIOD_DAY);
        
        init(PERIOD_DAY, PERIOD_HOUR);
        //some rolls passed
        spyLocalRecorder.record(Record.ROLL, _24D16H, 0, END_OF_ROLL, _24D);
        setCurrentRotation(_24D, PERIOD_DAY);
        
        rollTs = Util.getLatestRollTsUnderTimeBuf(_24D18H, PERIOD_HOUR, 0);
        assertEquals(_24D17H, rollTs);
        assertEquals(START_OF_NEXT_ROLL, spyReader.computeStartOffset(rollTs, PERIOD_DAY));
        
        rotateTsAssociateWithRollPeriod = Util.getLatestRollTsUnderTimeBuf(_25D, PERIOD_HOUR, 0);
        assertEquals(_24D23H, rotateTsAssociateWithRollPeriod);
        assertEquals(START_OF_NEXT_ROLL, spyReader.computeStartOffset(rotateTsAssociateWithRollPeriod, PERIOD_DAY));
        setCurrentRotation(_25D, PERIOD_DAY);
        
        //evaluate rotation happen at 2015-08-26 17:00:00 if watching file not increased
        rollTs = Util.getLatestRollTsUnderTimeBuf(_26D18H, PERIOD_HOUR, 0);
        assertEquals(_26D17H, rollTs);
        assertEquals(START_OF_NEXT_ROLL, spyReader.computeStartOffset(rollTs, PERIOD_DAY));
        
        //evaluate rotation happen at 2015-08-26 00:00:00 if watching file normal increased
        //notice rotate should trigger by file normal increasing
        setCurrentRotation(_26D, PERIOD_DAY);
        assertEquals(START_OF_NEW_ROTATE, spyReader.computeStartOffset(rollTs, PERIOD_DAY));
    }

    private long setCurrentRotation(long rotateHappenTime, long rotatePeriod) {
        long currentRotation = Util.getCurrentRotationUnderTimeBuf(rotateHappenTime, rotatePeriod, 0);
        spyReader.setCurrentRotation(currentRotation);
        return currentRotation;
    }
}

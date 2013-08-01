package com.dp.blackhole.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.blackhole.common.Util;

public class TestUtil {
    private static String[] unitStr;
    static {
        unitStr = new String[4];
        unitStr[0] = "hour";
        unitStr[1] = "day";
        unitStr[2] = "minute";
        unitStr[3] = "unknow";
    }
    
    enum MONTH {
        JAN,
        FEB,
        MAR,
        APR,
        MAY,
        JUN,
        JULY,
        AUG,
        SEP,
        OCT,
        NOV,
        DEC,
    }
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        com.dp.blackhole.simutil.Util.deleteTmpFile(
                com.dp.blackhole.simutil.Util.FILE_SUFFIX);
    }

    @Test
    public void testGetRollIdentByTime() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetOneHoursAgoTime() {
        Date setDate = new Date(2013, MONTH.JULY.ordinal(), 15, 18, 30, 24);
        Date expectedDate = new Date(2013, MONTH.JULY.ordinal(), 15, 17, 30, 24);
        assertEquals(expectedDate, Util.getOneHoursAgoTime(setDate));
        setDate = new Date(2013, MONTH.MAR.ordinal(), 1, 0, 0, 0);
        expectedDate = new Date(2013, MONTH.FEB.ordinal(), 28, 23, 0, 0);
        assertEquals(expectedDate, Util.getOneHoursAgoTime(setDate));
        setDate = new Date(2013, MONTH.JULY.ordinal(), 15, 00, 30, 24);
        expectedDate = new Date(2013, MONTH.JULY.ordinal(), 14, 23, 30, 24);
        assertEquals(expectedDate, Util.getOneHoursAgoTime(setDate));
        setDate = new Date(2013, MONTH.JAN.ordinal(), 1, 00, 30, 24);
        expectedDate = new Date(2012, MONTH.DEC.ordinal(), 31, 23, 30, 24);
        assertEquals(expectedDate, Util.getOneHoursAgoTime(setDate));
    }

    @Test
    public void testGetFormatByUnit() {
        String[] expecteds = new String[4];
        expecteds[0] = "yyyy-MM-dd.hh";
        expecteds[1] = "yyyy-MM-dd";
        expecteds[2] = "yyyy-MM-dd.hh:mm";
        expecteds[3] = "yyyy-MM-dd.hh";
        for(int i = 0; i<unitStr.length; i++) {
            assertEquals(expecteds[i], Util.getFormatByUnit(unitStr[i]));
        }
    }

    @Test
    public void testFindRealFileByIdent() throws FileNotFoundException, IOException {
        File file = com.dp.blackhole.simutil.Util.createTmpFile(
                com.dp.blackhole.simutil.Util.FILE_SUFFIX, "ok");
        assertNotNull(Util.findRealFileByIdent(
                file.getAbsolutePath(), com.dp.blackhole.simutil.Util.FILE_SUFFIX));
        com.dp.blackhole.simutil.Util.createTmpFile(
                com.dp.blackhole.simutil.Util.FILE_SUFFIX, "ok");
        assertNull(Util.findRealFileByIdent(
                file.getAbsolutePath(), com.dp.blackhole.simutil.Util.FILE_SUFFIX));
        com.dp.blackhole.simutil.Util.deleteTmpFile(
                com.dp.blackhole.simutil.Util.FILE_SUFFIX);
    }
}

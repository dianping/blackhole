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
    private static final String filepathname = "/tmp/893jfc842.log.2013-01-01.15";
    private static File file;
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
       file = new File(filepathname);
       file.createNewFile();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
       file.delete();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
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
    public void testFindRealFileByIdent() throws FileNotFoundException, IOException {
        File file = Util.findRealFileByIdent("/tmp/893jfc842.log", com.dp.blackhole.simutil.Util.FILE_SUFFIX);
        assertNotNull(file);
    }
}

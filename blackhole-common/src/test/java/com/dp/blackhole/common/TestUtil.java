package com.dp.blackhole.common;

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
    private static final long PEROID_OF_HOUR = 3600l;
    private static final long PEROID_OF_DAY = 3600 * 24l;
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
        File file = Util.findRealFileByIdent("/tmp/893jfc842.log", "2013-01-01.15");
        assertNotNull(file);
    }

    @Test
    public void testFindGZFileByIdent() throws IOException {
        File gzFile = new File("/tmp/hostname__appname.893jfc842.log.2013-01-01.15.gz");
        gzFile.createNewFile();
        File file = Util.findGZFileByIdent("/tmp/893jfc842.log", "2013-01-01.15");
        assertNotNull(file);
        gzFile.delete();
        gzFile = new File("/tmp/hostname__893jfc842.log.2013-01-01.15.gz");
        gzFile.createNewFile();
        file = Util.findGZFileByIdent("/tmp/893jfc842.log", "2013-01-01.15");
        assertNotNull(file);
        gzFile.delete();
    }

    @Test
    public void testGetCurrentRollTs() {
        long same = 1386950400000l;     //2013-12-14 00:00:00
        long diff1 = 1386950400001l;    //2013-12-14 00:00:01
        long diff2 = 1386950399999l;    //2013-12-13 23:59:59
        long result = Util.getCurrentRollTs(same, PEROID_OF_HOUR);
        assertEquals(same, result);
        assertFalse(diff1 == result);
        assertFalse(diff2 == result);
        result = Util.getCurrentRollTs(same, PEROID_OF_DAY);
        assertEquals(same, result);
        assertFalse(diff1 == result);
        assertFalse(diff2 == result);
    }

    @Test
    public void testGetLatestRotateRollTsUnderTimeBuf() {
        long beforeAndInBuf = 1386950395000l; //2013-12-13 23:59:55
        long beforeAndOutBuf = 1386950394000l; //2013-12-13 23:59:54
        long afterAndInBuf = 1386950401000l; //2013-12-14 00:00:01
        long testValue = 1378443602000l;
        long bufMs = 5000l;
        long result = Util.getLatestRotateRollTsUnderTimeBuf(beforeAndInBuf, PEROID_OF_HOUR, bufMs);
        assertEquals(1386946800000l, result);
        result = Util.getLatestRotateRollTsUnderTimeBuf(beforeAndOutBuf, PEROID_OF_HOUR, bufMs);
        assertEquals(1386943200000l, result);
        result = Util.getLatestRotateRollTsUnderTimeBuf(afterAndInBuf, PEROID_OF_HOUR, bufMs);
        assertEquals(1386946800000l, result);
        result = Util.getLatestRotateRollTsUnderTimeBuf(beforeAndInBuf, PEROID_OF_DAY, bufMs);
        assertEquals(1386864000000l, result);
        result = Util.getLatestRotateRollTsUnderTimeBuf(beforeAndOutBuf, PEROID_OF_DAY, bufMs);
        assertEquals(1386777600000l, result);
        result = Util.getLatestRotateRollTsUnderTimeBuf(afterAndInBuf, PEROID_OF_DAY, bufMs);
        assertEquals(1386864000000l, result);
        result = Util.getLatestRotateRollTsUnderTimeBuf(testValue, PEROID_OF_HOUR, bufMs);
        assertEquals(1378440000000l, result);
        result = Util.getLatestRotateRollTsUnderTimeBuf(testValue, PEROID_OF_DAY, bufMs);
        assertEquals(1378310400000l, result);
    }
    
    @Test
    public void testGetLionValueOfStringList() {
        String[] hosts = new String[3];
        hosts[0] = "test-somehost-web01.nh";
        hosts[1] = "test-somehost-web02.nh";
        hosts[2] = "test-somehost-web03.nh";
        String expectString = "[\"test-somehost-web01.nh\",\"test-somehost-web02.nh\",\"test-somehost-web03.nh\"]";
        assertEquals(expectString, Util.getLionValueOfStringList(hosts));
    }
    
    @Test
    public void testGetStringListOfLionValue() {
        String lionValue = "[ \"test-somehost-web01.nh\" , \"test-somehost-web02.nh\" , \"test-somehost-web03.nh\" ]";
        String[] hosts = Util.getStringListOfLionValue(lionValue);
        for (int i = 0; i < hosts.length; i++) {
            assertEquals("test-somehost-web0" + (i + 1) + ".nh", hosts[i]);
        }
    }
}

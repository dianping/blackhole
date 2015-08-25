package com.dp.blackhole.common;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
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
    private static final long PERIOD_OF_HOUR = 3600l;
    private static final long PERIOD_OF_DAY = 3600 * 24l;
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
        long result = Util.getCurrentRollTs(same, PERIOD_OF_HOUR);
        assertEquals(same, result);
        assertFalse(diff1 == result);
        assertFalse(diff2 == result);
        result = Util.getCurrentRollTs(same, PERIOD_OF_DAY);
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
        long result = Util.getLatestRollTsUnderTimeBuf(beforeAndInBuf, PERIOD_OF_HOUR, bufMs);
        assertEquals(1386946800000l, result);
        result = Util.getLatestRollTsUnderTimeBuf(beforeAndOutBuf, PERIOD_OF_HOUR, bufMs);
        assertEquals(1386943200000l, result);
        result = Util.getLatestRollTsUnderTimeBuf(afterAndInBuf, PERIOD_OF_HOUR, bufMs);
        assertEquals(1386946800000l, result);
        result = Util.getLatestRollTsUnderTimeBuf(beforeAndInBuf, PERIOD_OF_DAY, bufMs);
        assertEquals(1386864000000l, result);
        result = Util.getLatestRollTsUnderTimeBuf(beforeAndOutBuf, PERIOD_OF_DAY, bufMs);
        assertEquals(1386777600000l, result);
        result = Util.getLatestRollTsUnderTimeBuf(afterAndInBuf, PERIOD_OF_DAY, bufMs);
        assertEquals(1386864000000l, result);
        result = Util.getLatestRollTsUnderTimeBuf(testValue, PERIOD_OF_HOUR, bufMs);
        assertEquals(1378440000000l, result);
        result = Util.getLatestRollTsUnderTimeBuf(testValue, PERIOD_OF_DAY, bufMs);
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
    
    @Test
    public void testToAtomString() {
        String test1 = Util.toTupleString(null);
        String expect1 = null;
        assertEquals(test1, expect1);
        
        String test2 = Util.toTupleString("id", "name");
        String expect2 = "{id,name}";
        assertEquals(test2, expect2);
        
        String test3 = Util.toTupleString("id", 123);
        String expect3 = "{id,123}";
        assertEquals(test3, expect3);
    }
    
    @Test
    public void testFindHeadOfLastLine() throws FileNotFoundException, IOException {
        String MAGIC = "testFindHeadOfLastLine";
        File testFile = createTmpFile(MAGIC, "9sdfadfffadff");
        RandomAccessFile reader = new RandomAccessFile(testFile, "r");
        int testBufSize = 8;
        assertEquals(61L, Util.seekLastLineHeader(reader, 99L, testBufSize));
        reader.seek(61);
        assertTrue("1".getBytes()[0] == reader.readByte());
        assertEquals(360L, Util.seekLastLineHeader(reader, 400L, testBufSize));
        reader.seek(360);
        assertTrue("6".getBytes()[0] == reader.readByte());
        assertEquals(0L, Util.seekLastLineHeader(reader, 7L, testBufSize));
        reader.seek(0);
        assertTrue("0".getBytes()[0] == reader.readByte());
        assertEquals(551L, reader.length());
        assertEquals(537L, Util.seekLastLineHeader(reader, reader.length(), testBufSize));
        assertEquals(537L, reader.getFilePointer());
        assertTrue("9".getBytes()[0] == reader.readByte());
        reader.close();
        testFile.delete();
    }
    
    private static File createTmpFile(String MAGIC, String expected) 
            throws IOException, FileNotFoundException {
        String string = 
                "0egin>    owefoq jfojnofownfowofnownefowoefojweofjwosfnvvoco\n" +
                "1lsdfpasjdfaopsdpfaskdfkpasdkpfkasdfas     100>     jcsopdnvon\n" +
                "2ononoifjopwejf opwjfiop jpwj fopqwejfop qjfopiqjqertgbrtg\n" +
                "3spd jfoiasj df ioajsiodf asj fasof jasdopjf pasfj asopfjo\n" +
                "4tgrtghrthrthrthrhrthtrp sjfop asdj fopasj fopsfjopsjf wef\n" +
                "5 faiosjf opwqejo fjopwej faeopsf jopawefj opsjf opsafj ao\n" +
                "6wopejf opwj efopqwj epo fjwopefj pwef opw ejfopwj efopwf \n" +
                "7 wjopef joiqwf io j 9049 fj2490r 0pjfioj fioj qiowegio f \n" +
                "8f90fj 9034u j90 jgioqpwejf iopwe jfopqwefj opewji fopq934\n" +
                expected + "\n";
        //build a app log
        File file = new File("/tmp/" + MAGIC);
        file.createNewFile();
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file)));
        writer.write(string);
        writer.close();
        return file;
    }
    
    @Test
    public void testIsRollConcurrentWithRotate() {
        long rollPeriod1 = 300;
        long rotatePeriod1 = 3600;
        long currentTs1 = 1421683200070L;
        assertTrue(Util.isRollConcurrentWithRotate(currentTs1, rollPeriod1, rotatePeriod1));
        long rollPeriod2 = 3600;
        long rotatePeriod2 = 86400;
        long currentTs2 = 1421683200070L;
        assertTrue(Util.isRollConcurrentWithRotate(currentTs2, rollPeriod2, rotatePeriod2));
    }
    
    @Test
    public void testGetLatestRotateTsUnderTimeBuf() {
        long minutelyPeriod = 300;
        long hourlyPeriod = 3600;
        long daylyPeriod = 86400;
        long currentTs = 1421683200070L;            //2015-01-20 00:00:00
        long exceptCurrentRotation = 1421683200000L;
        long exceptValueHourly = 1421679600000L;    //2015-01-19 23:00:00
        long exceptValueMinutely = 1421682900000L;  //2015-01-19 23:55:00
        long exceptValueDayly = 1421596800000L;     //2015-01-19 00:00:00
        assertEquals(exceptCurrentRotation, Util.getCurrentRotationUnderTimeBuf(currentTs, hourlyPeriod, 200));
        assertEquals(exceptValueHourly, Util.getLatestRollTsUnderTimeBuf(currentTs, hourlyPeriod, 200));
        assertEquals(exceptValueMinutely, Util.getLatestRollTsUnderTimeBuf(currentTs, minutelyPeriod, 200));
        assertEquals(exceptCurrentRotation, Util.getCurrentRotationUnderTimeBuf(currentTs, daylyPeriod, 200));
        assertEquals(exceptValueDayly, Util.getLatestRollTsUnderTimeBuf(currentTs, daylyPeriod, 200));
        assertEquals(exceptValueHourly, Util.getLatestRollTsUnderTimeBuf(currentTs, hourlyPeriod, 200));
    }
    
    @Test
    public void testBelongToSameRotate() {
        assertFalse(Util.belongToSameRotate(1441080300000L, 1441079400000L, 3600));
    }
}

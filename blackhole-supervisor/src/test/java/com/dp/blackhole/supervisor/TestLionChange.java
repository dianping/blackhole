package com.dp.blackhole.supervisor;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLionChange {
    
    private LionConfChange lionConfChange;

    @Before
    public void setUp() throws Exception {
        lionConfChange = new LionConfChange(null, 71);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGenerateGetURL() {
        String exceptedString = "http://lionapi.dp:8080/getconfig?&p=blackhole&e=dev&id=71&k=testKey";
        assertEquals(exceptedString, lionConfChange.generateGetURL("testKey"));
    }

    @Test
    public void testGenerateSetURL() {
        String exceptedString = "http://lionapi.dp:8080/setconfig?&p=blackhole&e=dev&id=71&ef=1&k=testKey&v=testValue";
        assertEquals(exceptedString, lionConfChange.generateSetURL("testKey", "testValue"));
    }

}

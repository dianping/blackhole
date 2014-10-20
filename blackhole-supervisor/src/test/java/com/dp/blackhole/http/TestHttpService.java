package com.dp.blackhole.http;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.common.Util;
import com.dp.blackhole.http.HttpClientSingle;
import com.dp.blackhole.http.RequestListener;
import com.dp.blackhole.supervisor.ConfigManager;

@RunWith(MockitoJUnitRunner.class)
public class TestHttpService {
    
    private final static String marineapp = "topic1";
    private final static String nginxapp = "topic2";
    private final static String errorapp = "topic3";
    private final static String nullapp = "topic3";
    private final static String marineResp = "[\"test-good-web01.nh\",\"test-good-web02.nh\"]";
    private final static String nginxResp =  "[\"test-good-nginx01.nh\",\"test-good-nginx02.nh\"]";
    private final static String marineNewValue = "[\"test-good-web01.nh\",\"test-good-web02.nh\",\"new-host.nh\"]";
    private final static String nginxNewValue = "[\"test-good-nginx01.nh\",\"test-good-nginx02.nh\",\"new-host.nh\"]";
    private final static String errorResp = "1|lion error msg";
    private final static String nullResp = "<null>";
    private RequestListener listener;
    private int webServicePort = 28080;
    private final static String newHosts = "new-host1.nh,new-host2.nh";
    
    @Mock
    ConfigManager mockLion;
    @Mock
    Util mockUtil;
    @Mock
    HttpClientSingle mockHttpClient;
    
    @Before
    public void setUp() throws Exception {
        listener = new RequestListener(mockLion);
        listener.setDaemon(true);
        listener.start();
    }

    @After
    public void tearDown() throws Exception {
        listener.interrupt();
    }

    @SuppressWarnings("static-access")
    private void simulateLionGetUrl(String testurl, String response, String newValue) {
        when(mockUtil.generateGetURL(ParamsKey.LionNode.HOSTS_PREFIX + testurl)).thenReturn("http://testlion/get/" + testurl);
        when(mockUtil.generateSetURL(eq(ParamsKey.LionNode.HOSTS_PREFIX + testurl), anyString())).thenReturn("http://testlion/set/" + testurl);
        if (testurl.equals(marineapp)) {
            when(mockUtil.generateSetURL(ParamsKey.LionNode.HOSTS_PREFIX + testurl, marineNewValue)).thenReturn("http://testlion/set/" + testurl);
        } else if (testurl.equals(nginxapp)) {
            when(mockUtil.generateSetURL(ParamsKey.LionNode.HOSTS_PREFIX + testurl, nginxNewValue)).thenReturn("http://testlion/set/" + testurl);
        }
        when(mockHttpClient.getResponseText("http://testlion/get/" + testurl)).thenReturn(response);
        when(mockHttpClient.getResponseText("http://testlion/set/" + testurl)).thenReturn("0");
    }
    
    @Test
    public void test() {
        Set<String> rightAppList = new CopyOnWriteArraySet<String>();
        Set<String> errorAppList = new CopyOnWriteArraySet<String>();
        rightAppList.add(marineapp);
        rightAppList.add(nginxapp);
        when(mockLion.getTopicsByCmdb("noneed")).thenReturn(errorAppList);
        when(mockLion.getTopicsByCmdb("cmdbapp")).thenReturn(rightAppList);
        simulateLionGetUrl(marineapp, marineResp, marineNewValue);
        simulateLionGetUrl(nginxapp, nginxResp, nginxNewValue);
        simulateLionGetUrl(errorapp, errorResp, null);
        simulateLionGetUrl(nullapp, nullResp, null);
        String rightUrl = "http://localhost:" + webServicePort + "/scaleout?app=cmdbapp&hosts=" + newHosts;
        String errorUrl = "http://localhost:" + webServicePort + "/scaleout?app=noneed&hosts=" + newHosts;
        HttpClientSingle myTestClient = new HttpClientSingle(2000, 2000);
        String response = myTestClient.getResponseText(rightUrl);
        assertEquals("0|", response);
        response = myTestClient.getResponseText(errorUrl);
        assertEquals("1|It contains no mapping for the cmdbapp noneed", response);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

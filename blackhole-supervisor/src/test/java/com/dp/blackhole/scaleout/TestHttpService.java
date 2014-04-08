package com.dp.blackhole.scaleout;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.supervisor.LionConfChange;

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
    private final static String newHost = "new-host.nh";
    
    @Mock
    LionConfChange mockLion;
    @Mock
    HttpClientSingle mockHttpClient;
    
    @Before
    public void setUp() throws Exception {
        listener = new RequestListener(webServicePort, mockLion, mockHttpClient);
        listener.setDaemon(true);
        listener.start();
    }

    @After
    public void tearDown() throws Exception {
        listener.interrupt();
    }

    private String httpClientExec(HttpClientSingle myHttpClient, String url) {
        StringBuilder responseBuilder = new StringBuilder();
        BufferedReader bufferedReader = null;
        InputStream is = null;
        try {
            is = myHttpClient.getResource(url);
        } catch (IOException e) {
            return null;
        }
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(is, "UTF-8"), 8 * 1024);
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                responseBuilder.append(line + "\n");
            }
            if (responseBuilder.length() != 0) {
                responseBuilder.deleteCharAt(responseBuilder.length() - 1);
            }
        } catch (IOException e) {
            return null;
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
            }
        }
        return responseBuilder.toString();
    }
    
    private void simulateLionGetUrl(String testurl, String response, String newValue) {
        when(mockLion.generateGetURL(ParamsKey.LionNode.APP_HOSTS_PREFIX + testurl)).thenReturn("http://testlion/get/" + testurl);
        when(mockLion.generateSetURL(eq(ParamsKey.LionNode.APP_HOSTS_PREFIX + testurl), anyString())).thenReturn("http://testlion/set/" + testurl);
        if (testurl.equals(marineapp)) {
            when(mockLion.generateSetURL(ParamsKey.LionNode.APP_HOSTS_PREFIX + testurl, marineNewValue)).thenReturn("http://testlion/set/" + testurl);
        } else if (testurl.equals(nginxapp)) {
            when(mockLion.generateSetURL(ParamsKey.LionNode.APP_HOSTS_PREFIX + testurl, nginxNewValue)).thenReturn("http://testlion/set/" + testurl);
        }
        ByteArrayInputStream contentFromGetUri = new ByteArrayInputStream(response.getBytes());
        ByteArrayInputStream contentFromSetUri = null;
        if (newValue != null) {
            contentFromSetUri = new ByteArrayInputStream("0".getBytes());
        }
        try {
            when(mockHttpClient.getResource("http://testlion/get/" + testurl)).thenReturn(contentFromGetUri);
            when(mockHttpClient.getResource("http://testlion/set/" + testurl)).thenReturn(contentFromSetUri);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void test() {
        List<String> goodAppList = new ArrayList<String>();
        goodAppList.add(marineapp);
        goodAppList.add(nginxapp);
        Map<String, List<String>> mockCmdbMap = new HashMap<String, List<String>>();
        mockCmdbMap.put("cmdbapp", goodAppList);
        when(mockLion.getCmdbMapping()).thenReturn(mockCmdbMap);
        simulateLionGetUrl(marineapp, marineResp, marineNewValue);
        simulateLionGetUrl(nginxapp, nginxResp, nginxNewValue);
        simulateLionGetUrl(errorapp, errorResp, null);
        simulateLionGetUrl(nullapp, nullResp, null);
        String rightUrl = "http://localhost:" + webServicePort + "/scaleout?app=cmdbapp&host=" + newHost;
        String errorUrl = "http://localhost:" + webServicePort + "/scaleout?app=noneed&host=" + newHost;
        HttpClientSingle myTestClient = new HttpClientSingle(2000, 2000);
        String response = httpClientExec(myTestClient, rightUrl);
        assertEquals("0|", response);
        response = httpClientExec(myTestClient, errorUrl);
        assertEquals("1|It contains no mapping for the cmdbapp noneed", response);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

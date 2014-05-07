package com.dp.blackhole.scaleout;

public class HttpResult {
    public static final String SUCCESS = "0";
    public static final String FAILURE = "-1";
    public static final String NONEED = "1";
    String code;
    String msg;
    
    public HttpResult(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }
}
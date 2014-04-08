package com.dp.blackhole.scaleout;

public abstract class HttpAbstractHandler {
    public static final String SUCCESS = "SUCCESS";
    public static final String NONEED = "NONEED";
    public static final String FAILURE = "FAILURE";
    public abstract HttpResult getContent(String args1, String args2);
}

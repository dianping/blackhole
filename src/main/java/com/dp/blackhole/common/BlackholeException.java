package com.dp.blackhole.common;

public class BlackholeException extends Exception{
    private static final long serialVersionUID = 1L;

    public BlackholeException(String msg) {
      super(msg);
    }

    public BlackholeException(String msg, Throwable th) {
      super(msg, th);
    }

    public BlackholeException(Throwable th) {
      super(th);
    }
}

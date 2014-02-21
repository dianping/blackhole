package com.dp.blackhole.exception;

public class BlackholeClientException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public BlackholeClientException() {
        super();
    }

    public BlackholeClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public BlackholeClientException(String message) {
        super(message);
    }

    public BlackholeClientException(Throwable cause) {
        super(cause);
    }
}

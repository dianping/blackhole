package com.dp.blackhole.consumer.exception;

public class ConsumerTimeoutException extends Exception {

    private static final long serialVersionUID = 1L;

    public ConsumerTimeoutException() {
    }


    public ConsumerTimeoutException(String message) {
        super(message);
    }

    public ConsumerTimeoutException(Throwable cause) {
        super(cause);
    }

    public ConsumerTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}

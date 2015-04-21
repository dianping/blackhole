package com.dp.blackhole.consumer.exception;

public class ConsumerInitializeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ConsumerInitializeException() {
    }


    public ConsumerInitializeException(String message) {
        super(message);
    }

    public ConsumerInitializeException(Throwable cause) {
        super(cause);
    }

    public ConsumerInitializeException(String message, Throwable cause) {
        super(message, cause);
    }
}

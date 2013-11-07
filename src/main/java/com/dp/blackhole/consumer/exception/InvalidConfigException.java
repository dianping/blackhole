package com.dp.blackhole.consumer.exception;

public class InvalidConfigException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;

    public InvalidConfigException() {
        super();
    }

    public InvalidConfigException(String message) {
        super(message);
    }
}

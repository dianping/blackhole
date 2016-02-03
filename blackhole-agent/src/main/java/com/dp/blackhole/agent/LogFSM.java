package com.dp.blackhole.agent;

import java.util.concurrent.atomic.AtomicReference;

public class LogFSM {
    public enum LogState {NEW, APPEND, ROLL, ROTATE, HALT, FINISHED}
    private AtomicReference<LogState> currentLogState = new AtomicReference<LogState>(LogState.NEW);
    
    public LogState getCurrentLogStatus() {
        return currentLogState.get();
    }
    
    public void resetCurrentLogStatus() {
        currentLogState.set(LogState.NEW);
    }

    public void doFileAppend() {
        currentLogState.compareAndSet(LogState.APPEND, LogState.APPEND);
    }
    
    public void doFileAppendForce() {
        currentLogState.set(LogState.APPEND);
    }
    
    public void beginLogRotate() {
        if (currentLogState.get() == LogState.FINISHED
                || currentLogState.get() == LogState.HALT) {
            return;
        }
        currentLogState.set(LogState.ROTATE);
    }
    
    public void beginHalt() {
        currentLogState.set(LogState.HALT);
    }
    
    @Deprecated
    public void beginRollAttempt() {
        currentLogState.set(LogState.ROLL);
    }
    
    public void finishLogRotate() {
        currentLogState.compareAndSet(LogState.ROTATE, LogState.APPEND);
    }
    
    public void finishHalt() {
        currentLogState.compareAndSet(LogState.HALT, LogState.FINISHED);
    }
    
    public void finishRoll() {
        currentLogState.compareAndSet(LogState.ROLL, LogState.APPEND);
    }
}

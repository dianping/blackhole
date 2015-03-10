package com.dp.blackhole.agent;

import java.util.concurrent.atomic.AtomicReference;

public class LogFSM {
    public enum LogState {NEW, APPEND, ROLL, ROTATE, LAST_ROLL, FINISHED}
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
        currentLogState.set(LogState.ROTATE);
    }
    
    public void beginLastLogRotate() {
        currentLogState.set(LogState.LAST_ROLL);
    }
    
    public void beginRollAttempt() {
        currentLogState.set(LogState.ROLL);
    }
    
    public void finishLogRotate() {
        currentLogState.compareAndSet(LogState.ROTATE, LogState.APPEND);
    }
    
    public void finishLastRoll() {
        currentLogState.compareAndSet(LogState.LAST_ROLL, LogState.FINISHED);
    }
    
    public void finishRoll() {
        currentLogState.compareAndSet(LogState.ROLL, LogState.APPEND);
    }
}

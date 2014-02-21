package com.dp.blackhole.common;

public abstract class Schedule extends Thread {
    private boolean running = true;
    private long sleepDuration;

    public Schedule(long sleepDuration) {
        this.sleepDuration = sleepDuration;
    }

    public void changePeriod(long newSleepDuration) {
        this.sleepDuration = newSleepDuration;
    }

    public void run() {
        while (this.running)
            try {
                action();
                Thread.sleep(this.sleepDuration);
            } catch (InterruptedException e) {
                this.running = false;
            }
    }

    public abstract void action();
}
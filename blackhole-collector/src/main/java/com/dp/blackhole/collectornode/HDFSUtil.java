package com.dp.blackhole.collectornode;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtil {
    private static final int REPEATE = 3;
    private static final int RETRY_SLEEP_TIME = 3000;
    public static boolean retryDelete(FileSystem fs, Path path) {
        for (int i = 0; i < REPEATE; i++) {
            try {
                if (fs.delete(path, false)) {
                    return true;
                }
            } catch (IOException e) {
            }
            try {
                Thread.sleep(RETRY_SLEEP_TIME);
            } catch (InterruptedException ex) {
                return false;
            }
        }
        return false;
    }
    
    public static boolean retryRename(FileSystem fs, Path src, Path dst) {
        for (int i = 0; i < REPEATE; i++) {
            try {
                if (fs.rename(src, dst)) {
                    return true;
                }
            } catch (IOException e) {
            }
            try {
                Thread.sleep(RETRY_SLEEP_TIME);
            } catch (InterruptedException ex) {
                return false;
            }
        }
        return false;
    }
}

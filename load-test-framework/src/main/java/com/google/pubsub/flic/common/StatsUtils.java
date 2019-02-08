package com.google.pubsub.flic.common;

import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;

public class StatsUtils {
    /**
     * Returns the average QPS.
     */
    public static double getQPS(long messageCount, Duration loadtestDuration) {
        return messageCount / (double) Durations.toSeconds(loadtestDuration);
    }

    /**
     * Returns the average throughput in MB/s.
     */
    public static double getThroughput(long messageCount, Duration loadtestDuration, long messageSize) {
        return getQPS(messageCount, loadtestDuration) * messageSize / 1000000.0;
    }
}

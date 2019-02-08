package com.google.pubsub.flic.controllers.mode;

import com.google.protobuf.util.Durations;

import java.util.Optional;

public class WellKnownModes {
    public static Mode LATENCY_MODE = Mode.builder()
            .setMessageSize(1)
            .setPublishBatchSize(1)
            .setPublishRatePerSec(Optional.of(1))
            .setPublishBatchDuration(Durations.fromMillis(1))
            .setNumCoresPerWorker(1)
            .build();
    public static Mode THROUGHPUT_MODE = Mode.builder()
            .setNumCoresPerWorker(16)
            .build();
    public static Mode NOOP_MODE = Mode.builder()
            .setBurnInDuration(Durations.fromSeconds(0))
            .setLoadtestDuration(Durations.fromSeconds(0))
            .setNumCoresPerWorker(1)
            .build();
}

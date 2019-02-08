package com.google.pubsub.flic.controllers.mode;

import com.google.auto.value.AutoValue;

import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;

import java.util.Optional;

@AutoValue
public abstract class Mode {
    public abstract int messageSize();

    public abstract int publishBatchSize();

    public abstract Optional<Integer> publishRatePerSec();

    public abstract Duration loadtestDuration();

    public abstract Duration burnInDuration();

    public abstract Duration publishBatchDuration();

    public abstract int numCoresPerWorker();

    public abstract int numPublisherWorkers();

    public abstract int numSubscriberWorkers();

    public abstract int subscriberCpuScaling();

    public abstract Builder toBuilder();

    public static Builder builder() {
        return new AutoValue_Mode.Builder()
                .setMessageSize(1000)
                .setPublishBatchDuration(Durations.fromMillis(50))
                .setPublishBatchSize(1000)
                .setBurnInDuration(Durations.fromSeconds(2 * 60))
                .setLoadtestDuration(Durations.fromSeconds(10 * 60))
                .setNumPublisherWorkers(1)
                .setNumSubscriberWorkers(1)
                .setSubscriberCpuScaling(5);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract Builder setMessageSize(int messageSize);

        abstract Builder setPublishBatchSize(int publishBatchSize);

        abstract Builder setPublishRatePerSec(Optional<Integer> publishRatePerSec);

        abstract Builder setLoadtestDuration(Duration loadtestDuration);

        abstract Builder setBurnInDuration(Duration burnInDuration);

        abstract Builder setPublishBatchDuration(Duration publishBatchDuration);

        abstract Builder setNumCoresPerWorker(int numCoresPerWorker);

        abstract Builder setNumPublisherWorkers(int publisherWorkers);

        abstract Builder setNumSubscriberWorkers(int subscriberWorkers);

        abstract Builder setSubscriberCpuScaling(int subscriberCpuScaling);

        abstract Mode build();
    }
}

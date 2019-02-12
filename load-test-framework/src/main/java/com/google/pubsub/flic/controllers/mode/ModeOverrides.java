package com.google.pubsub.flic.controllers.mode;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.protobuf.util.Durations;

@Parameters(separators = "=")
public class ModeOverrides {
    @Parameter(
            names = {"--local"},
            description = "Run the test locally."
    )
    public boolean local = false;

    @Parameter(
            names = {"--message_size"},
            description = "Set the message size for test."
    )
    private Integer messageSize = null;

    @Parameter(
            names = {"--burn_in_minutes"},
            description = "Set the number of minutes to burn in for."
    )
    private Integer burnInMinutes = null;

    @Parameter(
            names = {"--test_minutes"},
            description = "Set the number of minutes to test for."
    )
    private Integer testMinutes = null;

    @Parameter(
            names = {"--num_cores"},
            description = "Set the number of cores per worker."
    )
    private Integer numCores = null;

    @Parameter(
        names = {"--scaling_factor"},
        description = "Set the subscriber scaling factor per core per worker."
    )
    private Integer scalingFactor = null;

    public Mode apply(Mode source) {
        Mode.Builder builder = source.toBuilder();
        if (messageSize != null) {
            builder.setMessageSize(messageSize);
        }
        if (burnInMinutes != null) {
            builder.setBurnInDuration(Durations.fromSeconds(burnInMinutes * 60));
        }
        if (testMinutes != null) {
            builder.setLoadtestDuration(Durations.fromSeconds(testMinutes * 60));
        }
        if (numCores != null) {
            builder.setNumCoresPerWorker(numCores);
        }
        if (scalingFactor != null) {
            builder.setSubscriberCpuScaling(scalingFactor);
        }
        return builder.build();
    }
}

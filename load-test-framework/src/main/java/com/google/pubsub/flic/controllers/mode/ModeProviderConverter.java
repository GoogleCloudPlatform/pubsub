package com.google.pubsub.flic.controllers.mode;

import com.beust.jcommander.IStringConverter;

public class ModeProviderConverter implements IStringConverter<ModeProvider> {
    @Override
    public ModeProvider convert(String value) {
        switch (value) {
            case "latency":
                return ModeProvider.of(WellKnownModes.LATENCY_MODE);
            case "throughput":
                return ModeProvider.of(WellKnownModes.THROUGHPUT_MODE);
            case "core-scaling":
                return new CoreScalingModeProvider(WellKnownModes.THROUGHPUT_MODE);
            case "message-size":
                return new MessageSizeScalingModeProvider(WellKnownModes.THROUGHPUT_MODE);
            case "thread-scaling":
                return new ScalingFactorModeProvider(WellKnownModes.THROUGHPUT_MODE);
            case "noop":
                return ModeProvider.of(WellKnownModes.NOOP_MODE);
        }
        return null;
    }
}

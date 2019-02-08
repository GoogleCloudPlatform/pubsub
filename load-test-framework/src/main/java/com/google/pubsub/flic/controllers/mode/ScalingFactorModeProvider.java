package com.google.pubsub.flic.controllers.mode;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class ScalingFactorModeProvider implements ModeProvider {
    private final Mode baseMode;

    ScalingFactorModeProvider(Mode baseMode) {
        this.baseMode = baseMode;
    }

    @Override
    public List<Mode> modes() {
        return ImmutableList.of(
                baseMode.toBuilder().setSubscriberCpuScaling(0).build(),
                baseMode.toBuilder().setSubscriberCpuScaling(1).build(),
                baseMode.toBuilder().setSubscriberCpuScaling(2).build(),
                baseMode.toBuilder().setSubscriberCpuScaling(4).build()
        );
    }
}

package com.google.pubsub.flic.controllers.mode;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class CoreScalingModeProvider implements ModeProvider {
    private final Mode baseMode;

    CoreScalingModeProvider(Mode baseMode) {
        this.baseMode = baseMode;
    }

    @Override
    public List<Mode> modes() {
        return ImmutableList.of(
                baseMode.toBuilder().setNumCoresPerWorker(1).build(),
                baseMode.toBuilder().setNumCoresPerWorker(2).build(),
                baseMode.toBuilder().setNumCoresPerWorker(4).build(),
                baseMode.toBuilder().setNumCoresPerWorker(8).build(),
                baseMode.toBuilder().setNumCoresPerWorker(16).build()
        );
    }
}

package com.google.pubsub.flic.controllers.mode;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class MessageSizeScalingModeProvider implements ModeProvider {
    private final Mode baseMode;

    MessageSizeScalingModeProvider(Mode baseMode) {
        this.baseMode = baseMode;
    }

    @Override
    public List<Mode> modes() {
        return ImmutableList.of(
                baseMode.toBuilder().setMessageSize(1000).build(),
                baseMode.toBuilder().setMessageSize(10000).build(),
                baseMode.toBuilder().setMessageSize(100000).build(),
                baseMode.toBuilder().setMessageSize(1000000).build()
        );
    }
}

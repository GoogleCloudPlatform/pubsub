package com.google.pubsub.flic.controllers.mode;

import com.google.common.collect.ImmutableList;

import java.util.List;

public interface ModeProvider {
    List<Mode> modes();

    static ModeProvider of(Mode mode) {
        return new OneModeProvider(mode);
    }

    class OneModeProvider implements ModeProvider {
        private final Mode mode;

        protected OneModeProvider(Mode mode) {
            this.mode = mode;
        }

        @Override
        public List<Mode> modes() {
            return ImmutableList.of(mode);
        }
    }
}

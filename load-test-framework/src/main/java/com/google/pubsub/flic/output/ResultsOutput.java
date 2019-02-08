package com.google.pubsub.flic.output;

import com.google.pubsub.flic.common.LatencyTracker;
import com.google.pubsub.flic.controllers.ClientType;
import com.google.pubsub.flic.controllers.mode.Mode;

import java.util.List;

public interface ResultsOutput {
    class TrackedResult {
        public Mode mode;
        public ClientType type;
        public LatencyTracker tracker;

        public TrackedResult(Mode mode, ClientType type, LatencyTracker tracker) {
            this.mode = mode;
            this.type = type;
            this.tracker = tracker;
        }
    }

    void outputStats(List<TrackedResult> results);
}

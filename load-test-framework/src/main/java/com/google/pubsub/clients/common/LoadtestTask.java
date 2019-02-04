package com.google.pubsub.clients.common;

import com.google.pubsub.flic.common.LoadtestProto;

/**
 * Each task is responsible for triggering its workers when it is run.  It controls its own parallelism.
 */
public interface LoadtestTask {
    // Start the task
    void start();
    // Stop the task
    void stop();

    // A factory for constructing a task.
    public interface Factory {
        LoadtestTask newTask(LoadtestProto.StartRequest request, MetricsHandler handler, int workerCount);
    }
}

package com.google.pubsub.flic.controllers;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.ScheduledExecutorService;

/** A ResourceController creates a resource when start is called and cleans it up when stop is called.
 *
 * These operations are complete when their future returns.
 * **/
public abstract class ResourceController {
    private final ScheduledExecutorService executor;

    ResourceController(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    public ListenableFuture<Void> start() {
        SettableFuture<Void> future = SettableFuture.create();
        executor.execute(() -> {
            try {
                startAction();
            } catch (Exception e) {
                future.setException(e);
                return;
            }
            future.set(null);
        });
        return future;
    }
    public ListenableFuture<Void> stop() {
        SettableFuture<Void> future = SettableFuture.create();
        executor.execute(() -> {
            try {
                stopAction();
            } catch (Exception e) {
                future.setException(e);
                return;
            }
            future.set(null);
        });
        return future;
    }

    protected abstract void startAction() throws Exception;
    protected abstract void stopAction() throws Exception;
}

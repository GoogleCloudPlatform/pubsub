/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.google.pubsub.clients.common;

import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A LoadtestTask that manages its actions using a thread pool. */
public abstract class PooledWorkerTask implements LoadtestTask {
  private static final Logger log = LoggerFactory.getLogger(PooledWorkerTask.class);

  protected final StartRequest request;
  protected final MetricsHandler metricsHandler;
  protected final int workerCount;
  protected final AtomicInteger runningWorkers;
  protected final AtomicBoolean isShutdown;
  private final ThreadPoolExecutor executor;
  private SettableFuture<Void> shutdownFuture;

  public PooledWorkerTask(StartRequest request, MetricsHandler metricsHandler, int workerCount) {
    this.request = request;
    this.metricsHandler = metricsHandler;
    this.workerCount = workerCount;
    this.runningWorkers = new AtomicInteger(0);
    this.isShutdown = new AtomicBoolean(true);
    this.executor =
        new ThreadPoolExecutor(
            workerCount, workerCount, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(workerCount));
    this.shutdownFuture = SettableFuture.create();
  }

  // Run the task. This should never return until shutdown is set.
  protected abstract void startAction();

  // Run actions that should be performed on shutdown.  isShutdown will be set and all workers
  // stopped before this is called.
  protected abstract void cleanup();

  @Override
  public void start() {
    if (!isShutdown.getAndSet(false)) {
      throw new RuntimeException("This cannot be re-run when not shut down.");
    }
    log.warn("starting workers");
    IntStream.range(0, workerCount)
        .forEach(
            n ->
                executor.execute(
                    () -> {
                      log.warn("worker started");
                      runningWorkers.incrementAndGet();
                      // Should run until isShutdown is set.
                      startAction();
                      if (!isShutdown.get()) {
                        throw new RuntimeException(
                            "Logic error: worker exited before shutdown was set.");
                      }
                      int existingWorkers = runningWorkers.decrementAndGet();
                      log.warn("workers remaining: " + existingWorkers);
                      if (existingWorkers == 0) {
                        shutdownFuture.set(null);
                      }
                    }));
  }

  // Block until no workers are running.
  @Override
  public void stop() {
    isShutdown.set(true);
    try {
      shutdownFuture.get();
    } catch (ExecutionException | InterruptedException e) {
      log.error("Failed to wait for shutdown: " + e);
    }
    executor.shutdown();
    cleanup();
  }
}

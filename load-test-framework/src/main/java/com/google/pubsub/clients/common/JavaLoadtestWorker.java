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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.flic.common.LoadtestProto.CheckRequest;
import com.google.pubsub.flic.common.LoadtestProto.CheckResponse;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.flic.common.LoadtestProto.StartResponse;
import com.google.pubsub.flic.common.LoadtestWorkerGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts a server to get the start request, then starts the load task. */
public class JavaLoadtestWorker {
  private static final Logger log = LoggerFactory.getLogger(JavaLoadtestWorker.class);

  private final Stopwatch stopwatch = Stopwatch.createUnstarted();
  private final LoadtestTask.Factory taskFactory;
  private final Server server;
  private Timestamp startTime;
  private LoadtestTask task;
  private MetricsHandler metricsHandler;
  private final AtomicBoolean finished = new AtomicBoolean(true);
  private SettableFuture<Void> finishedFuture = SettableFuture.create();

  public JavaLoadtestWorker(Options options, LoadtestTask.Factory taskFactory) throws Exception {
    this.taskFactory = taskFactory;

    ServerBuilder serverBuilder = ServerBuilder.forPort(options.port);

    server =
        serverBuilder
            .addService(
                new LoadtestWorkerGrpc.LoadtestWorkerImplBase() {
                  @Override
                  public void start(
                      StartRequest request, StreamObserver<StartResponse> responseObserver) {
                    startTime = request.getStartTime();
                    if (!finished.compareAndSet(true, false)) {
                      responseObserver.onError(new Exception("A load test is already running!"));
                      return;
                    }
                    Executors.newSingleThreadExecutor()
                        .submit(
                            () -> {
                              try {
                                runTest(request);
                              } catch (Exception e) {
                                log.error("Error in runTest: " + e);
                                e.printStackTrace();
                                System.exit(1);
                              }
                            });
                    responseObserver.onNext(StartResponse.getDefaultInstance());
                    responseObserver.onCompleted();
                  }

                  @Override
                  public void check(
                      CheckRequest request, StreamObserver<CheckResponse> responseObserver) {
                    boolean finishedValue = finished.get();
                    CheckResponse response = metricsHandler.check();
                    response =
                        response
                            .toBuilder()
                            .setRunningDuration(getTimeSinceStart())
                            .setIsFinished(finishedValue)
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    if (finishedValue) {
                      finishedFuture.set(null);
                    }
                  }
                })
            .build()
            .start();
    log.info("Started server, listening on port " + options.port + ".");
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (server != null) {
                    log.error("Shutting down server since JVM is shutting down.");
                    server.shutdown();
                  }
                }));
    // Deadlock forever, since we do not want the server to stop.
    Thread.currentThread().join();
  }

  /** Command line options for the {@link JavaLoadtestWorker}. */
  @Parameters(separators = "=")
  public static class Options {
    @Parameter(
        names = {"--port"},
        description = "The port to listen on.")
    public int port = 5000;
  }

  private void runTest(StartRequest request) {
    finishedFuture = SettableFuture.create();
    stopwatch.reset();
    metricsHandler = new MetricsHandler(request.getIncludeIds());
    log.info("Request received, starting up server.");
    int poolSize =
        Math.max(Runtime.getRuntime().availableProcessors() * request.getCpuScaling(), 1);
    task = taskFactory.newTask(request, metricsHandler, poolSize);

    long timeUntilStartMillis = -Durations.toMillis(getTimeSinceStart());
    log.info("starting in: " + Durations.fromMillis(timeUntilStartMillis));
    if (timeUntilStartMillis > 0) {
      try {
        Thread.sleep(timeUntilStartMillis);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    log.info("starting now");
    stopwatch.start();
    log.info("stopwatch started");
    task.start();
    log.info("task started");

    try {
      Thread.sleep(Durations.toMillis(request.getTestDuration()));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    log.info("test stopping");
    stopwatch.stop();
    finished.set(true);
    task.stop();
    log.info("Load test complete.");
  }

  private Duration getTimeSinceStart() {
    return Timestamps.between(startTime, Timestamps.fromMillis(System.currentTimeMillis()));
  }
}

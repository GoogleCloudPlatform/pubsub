// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////

package com.google.pubsub.clients.common;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.SettableFuture;

import com.google.protobuf.Duration;
import com.google.protobuf.util.Timestamps;

import com.google.pubsub.flic.common.LoadtestGrpc.LoadtestImplBase;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.flic.common.LoadtestProto.CheckRequest;
import com.google.pubsub.flic.common.LoadtestProto.CheckResponse;
import com.google.pubsub.flic.common.LoadtestProto.StartResponse;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest.StopConditionsCase;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.nio.charset.Charset;

import java.util.function.Function;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts a server to get the start request, then starts the load task.
 */
public class LoadTestRunner {
  private static Task task;
  private static Server server;
  private static final int MAX_IDLE_MILLIS = 60 * 1000; // 1 minute
  private static final Stopwatch stopwatch = Stopwatch.createUnstarted();
  private static SettableFuture<Void> finishedFuture = SettableFuture.create();
  private static final Logger log = LoggerFactory.getLogger(LoadTestRunner.class);
  private static final AtomicBoolean finished = new AtomicBoolean(true);

  /**
   * Command line options for the {@link LoadTestRunner}.
   */
  @Parameters(separators = "=")
  public static class Options {
    @Parameter(
      names = {"--port"},
      description = "The port to listen on."
    )
    public int port = 5000;
  }

  private static void runTest(StartRequest request) {
    log.info("Request received, starting up server.");
    int poolSize = request.getMaxOutstandingRequests() + 10;
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            poolSize,
            poolSize,
            100,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(poolSize),
            new ThreadPoolExecutor.CallerRunsPolicy());

    final long toSleep = request.getStartTime().getSeconds() * 1000 - System.currentTimeMillis();
    if (toSleep > 0) {
      try {
        Thread.sleep(toSleep);
      } catch (InterruptedException e) {
        log.error("Interrupted sleeping, starting test now.");
      }
    }
    stopwatch.start();
    while (shouldContinue(request)) {
      executor.execute(task);
    }
    log.info("Load test complete.");
    stopwatch.stop();
    finished.set(true);
    task.shutdown();
    executor.shutdownNow();
    log.info("Shutting down server.");
  }

  public static void run(Options options, Function<StartRequest, Task> function)
      throws Exception {
    run(options, function, null);
  }

  public static void run(
      Options options,
      Function<StartRequest, Task> function,
      ServerBuilder<?> serverBuilder)
      throws Exception {
    if (serverBuilder == null) {
      serverBuilder = ServerBuilder.forPort(options.port);
    }

    server =
        serverBuilder
            .addService(
                new LoadtestImplBase() {
                  @Override
                  public void start(
                      StartRequest request, StreamObserver<StartResponse> responseObserver) {
                    if (!finished.compareAndSet(true, false)) {
                      responseObserver.onError(new Exception("A load test is already running!"));
                    }
                    finishedFuture = SettableFuture.create();
                    stopwatch.reset();
                    task = function.apply(request);
                    Executors.newSingleThreadExecutor()
                        .submit(() -> LoadTestRunner.runTest(request));
                    responseObserver.onNext(StartResponse.getDefaultInstance());
                    responseObserver.onCompleted();
                  }

                  @Override
                  public void check(
                      CheckRequest request, StreamObserver<CheckResponse> responseObserver) {
                    boolean finishedValue = finished.get();
                    responseObserver.onNext(
                        CheckResponse.newBuilder()
                            .addAllReceivedMessages(
                                task.flushMessageIdentifiers(request.getDuplicatesList()))
                            .addAllBucketValues(task.getBucketValues())
                            .setRunningDuration(
                                Duration.newBuilder()
                                    .setSeconds(stopwatch.elapsed(TimeUnit.SECONDS)))
                            .setIsFinished(finishedValue)
                            .build());
                    responseObserver.onCompleted();
                    if (finishedValue) {
                      finishedFuture.set(null);
                    }
                  }
                })
            .build()
            .start();
    log.info("Started server, listening on port " + options.port + ".");
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (server != null) {
          log.error("Shutting down server since JVM is shutting down.");
          server.shutdown();
        }
      }
    });
    // Deadlock forever, since we do not want the server to stop.
    Thread.currentThread().join();
  }

  private static boolean shouldContinue(StartRequest request) {
    // If the test has been running for at least a minute, and we have been idle for a minute, we
    // should stop.
    if (System.currentTimeMillis() - Timestamps.toMillis(request.getStartTime()) > MAX_IDLE_MILLIS
        && System.currentTimeMillis() - task.getLastUpdateMillis() > MAX_IDLE_MILLIS) {
      return false;
    }
    switch (request.getStopConditionsCase()) {
      case TEST_DURATION:
        return System.currentTimeMillis()
            < (request.getStartTime().getSeconds()
                    + request.getBurnInDuration().getSeconds()
                    + request.getTestDuration().getSeconds())
                * 1000;
      case NUMBER_OF_MESSAGES:
        return task.getNumberOfMessages() < request.getNumberOfMessages()
            && task.getActualCounter() < request.getNumberOfMessages();
      default:
        return false;
    }
  }

  /**
   * Creates a string message of a certain size.
   */
  public static String createMessage(int msgSize) {
    byte[] payloadArray = new byte[msgSize];
    Arrays.fill(payloadArray, (byte) 'A');
    return new String(payloadArray, Charset.forName("UTF-8"));
  }
}

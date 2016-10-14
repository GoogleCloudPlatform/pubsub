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

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Duration;
import com.google.pubsub.flic.common.LoadtestGrpc;
import com.google.pubsub.flic.common.LoadtestProto.CheckRequest;
import com.google.pubsub.flic.common.LoadtestProto.CheckResponse;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.flic.common.LoadtestProto.StartResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Starts a server to get the start request, then starts the client runner.
 */
public class LoadTestRunner {
  private static final Logger log = LoggerFactory.getLogger(LoadTestRunner.class);
  private static final Stopwatch stopwatch = Stopwatch.createUnstarted();
  private static Server server;
  private static Task client;
  private static AtomicBoolean finished = new AtomicBoolean(true);
  private static SettableFuture<Void> finishedFuture = SettableFuture.create();

  private LoadTestRunner(StartRequest request) throws Exception {
    log.info("Request received, starting up server.");
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(request.getMaxOutstandingRequests() + 10));

    final RateLimiter rateLimiter = RateLimiter.create(request.getRequestRate());
    final Semaphore outstandingTestLimiter =
        new Semaphore(request.getMaxOutstandingRequests(), false);

    final long toSleep = request.getStartTime().getSeconds() * 1000 - System.currentTimeMillis();
    if (toSleep > 0) {
      Thread.sleep(toSleep);
    }

    stopwatch.start();
    while (shouldContinue(request)) {
      outstandingTestLimiter.acquireUninterruptibly();
      rateLimiter.acquire();
      executor.submit(client).addListener(outstandingTestLimiter::release, executor);
    }
    stopwatch.stop();
    executor.shutdownNow();
    finished.set(true);
    log.info("Load test complete.");
  }

  public static void run(Function<StartRequest, Task> function) throws Exception {
    server = ServerBuilder.forPort(5000)
        .addService(new LoadtestGrpc.LoadtestImplBase() {
          @Override
          public void start(StartRequest request, StreamObserver<StartResponse> responseObserver) {
            if (!finished.compareAndSet(true, false)) {
              responseObserver.onError(new Exception("A load test is already running!"));
            }
            finishedFuture = SettableFuture.create();
            stopwatch.reset();
            client = function.apply(request);
            Executors.newSingleThreadExecutor().submit(() -> new LoadTestRunner(request));
            responseObserver.onNext(StartResponse.getDefaultInstance());
            responseObserver.onCompleted();
          }

          @Override
          public void check(CheckRequest request, StreamObserver<CheckResponse> responseObserver) {
            boolean finishedValue = finished.get();
            responseObserver.onNext(CheckResponse.newBuilder()
                .addAllBucketValues(client.getBucketValues())
                .setRunningDuration(Duration.newBuilder()
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
    log.info("Started server, listening on port 5000.");
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
    switch (request.getStopConditionsCase()) {
      case TEST_DURATION:
        return System.currentTimeMillis() <
            (request.getStartTime().getSeconds() + request.getTestDuration().getSeconds()) * 1000;
      case NUMBER_OF_MESSAGES:
        return client.getNumberOfMessages() < request.getNumberOfMessages();
    }
    return false;
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

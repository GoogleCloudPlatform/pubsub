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
package com.google.pubsub.clients.gcloud;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.common.util.concurrent.*;
import com.google.protobuf.Empty;
import com.google.pubsub.flic.common.Command;
import com.google.pubsub.flic.common.LoadtestFrameworkGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Repeatedly starts {@link LoadTestRun LoadTestRuns} to generate load on the server.
 */
public class LoadTest {
  private static final Logger log = LoggerFactory.getLogger(LoadTest.class);
  @Parameter(names = {"--rate"},
      description = "Number of times per second to try execute a test run.")
  private double executionRate = 1000.0;
  @Parameter(names = {"--payload_size"},
      description = "Size in bytes of the data field per message")
  private int payloadSize = 1000;
  private Server server;

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit());
    LoadTest loadTest = new LoadTest();
    new JCommander(loadTest, args);
    loadTest.run();
  }

  private void run() throws Exception {
    SettableFuture<Command.CommandRequest> requestFuture = SettableFuture.create();
    server = ServerBuilder.forPort(5000)
        .addService(new LoadtestFrameworkGrpc.LoadtestFrameworkImplBase() {
          @Override
          public void startClient(Command.CommandRequest request, StreamObserver<Empty> responseObserver) {
            if (requestFuture.isDone()) {
              responseObserver.onError(new Exception("Start should only be called once, ignoring this request."));
              return;
            }
            requestFuture.set(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
          }
        })
        .build()
        .start();
    log.info("Started server, listening on port 5000.");
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        log.error("Shutting down server since JVM is shutting down.");
        if (server != null) {
          server.shutdown();
        }
        log.error("Server shut down.");
      }
    });

    Command.CommandRequest request = requestFuture.get();
    final int numWorkers = request.getNumberOfWorkers();
    LoadTestRun.batchSize = request.getMaxMessagesPerPull();
    LoadTestRun.subscription = request.getSubscription();
    LoadTestRun.topic = request.getTopic();
    final long sleepTime = request.getStartTime().getSeconds() * 1000 - System.currentTimeMillis();
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
    log.info("Request received, starting up server.");
    final PubSub pubSub = PubSubOptions.builder().projectId(request.getProject()).build().service();

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(numWorkers));

    log.info("Configured executor with " + numWorkers + " threads.");
    final byte[] payloadArray = new byte[payloadSize];
    Arrays.fill(payloadArray, (byte) 'A');
    final String payload = new String(payloadArray, Charset.forName("UTF-8"));

    log.info("Bringing up load test");
    final long endTimeMillis = request.getStopTime().getSeconds() * 1000;
    final RateLimiter rateLimiter = RateLimiter.create(executionRate);
    final Semaphore outstandingTestLimiter = new Semaphore(numWorkers, false);
    while (System.currentTimeMillis() < endTimeMillis) {
      outstandingTestLimiter.acquireUninterruptibly();
      rateLimiter.acquire();
      executor.submit(new LoadTestRun(pubSub, payload)).addListener(outstandingTestLimiter::release, executor);
    }
  }
}

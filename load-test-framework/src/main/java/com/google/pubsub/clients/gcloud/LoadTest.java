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
  double executionRate = 1000.0;
  @Parameter(names = {"--test_executor_num_threads"},
      description = "Number of threads in the executor that runs all test cases.")
  int testExecutorNumThreads = 1000;
  @Parameter(names = {"--project"}, description = "Project to use for load testing.")
  String project = "project";
  @Parameter(names = {"--topic"}, description = "Topic to use for load testing.")
  String topic = "load-test-topic";
  @Parameter(names = {"--subscription"},
      description = "Subscription to use for load testing. If set this client will Pull from the subscription. If not "
          + "set this client will Publish to the provided topic.")
  String subscription = "";
  @Parameter(names = {"--batch_size"},
      description = "Number of messages to batch per pull / publish request. ")
  int batchSize = 100;
  @Parameter(names = {"--payload_size"},
      description = "Size in bytes of the data field per message")
  int payloadSize = 1000;
  private Server server;

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit());
    LoadTest loadTest = new LoadTest();
    new JCommander(loadTest, args);
    LoadTestFlags.parse(loadTest);
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
    this.testExecutorNumThreads = request.getNumberOfWorkers();
    this.project = request.getProject();
    this.batchSize = request.getMaxMessagesPerPull();
    this.topic = request.getTopic();
    final long sleepTime = request.getStartTime().getSeconds() * 1000 - System.currentTimeMillis();
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
    log.info("Request received, starting up server.");
    final PubSub pubSub = PubSubOptions.builder().projectId(LoadTestFlags.project).build().service();

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(LoadTestFlags.testExecutorNumThreads));

    log.info("Configured executor with " + LoadTestFlags.testExecutorNumThreads + " threads.");
    final byte[] payloadArray = new byte[LoadTestFlags.payloadSize];
    Arrays.fill(payloadArray, (byte) 'A');
    final String payload = new String(payloadArray, Charset.forName("UTF-8"));

    log.info("Bringing up load test");
    final long endTimeMillis = request.getStopTime().getSeconds() * 1000;
    final RateLimiter rateLimiter = RateLimiter.create(LoadTestFlags.executionRate);
    final Semaphore outstandingTestLimiter = new Semaphore(LoadTestFlags.testExecutorNumThreads, false);
    while (System.currentTimeMillis() < endTimeMillis) {
      outstandingTestLimiter.acquireUninterruptibly();
      rateLimiter.acquire();
      executor.submit(new LoadTestRun(pubSub, payload)).addListener(outstandingTestLimiter::release, executor);
    }
  }
}

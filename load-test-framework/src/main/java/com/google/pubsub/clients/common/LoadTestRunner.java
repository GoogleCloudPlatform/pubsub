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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
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
import java.util.function.Function;

/**
 * Starts a server to get the start request, then starts the client runner.
 */
public class LoadTestRunner {
  private static final Logger log = LoggerFactory.getLogger(LoadTestRunner.class);
  private static Server server;

  public static void run(Function<Command.CommandRequest, Runnable> loadFunction) throws Exception {
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
        if (server != null) {
          log.error("Shutting down server since JVM is shutting down.");
          server.shutdown();
        }
      }
    });

    Command.CommandRequest request = requestFuture.get();
    log.info("Request received, starting up server.");
    final long toSleep = request.getStartTime().getSeconds() * 1000 - System.currentTimeMillis();
    if (request.hasStartTime() && toSleep > 0) {
      Thread.sleep(toSleep);
    }

    final int numWorkers = request.getNumberOfWorkers();
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(numWorkers));

    final long endTimeMillis = request.getStopTime().getSeconds() * 1000;
    final RateLimiter rateLimiter = RateLimiter.create(request.getRequestRate());
    final Semaphore outstandingTestLimiter = new Semaphore(numWorkers, false);
    Runnable client = loadFunction.apply(request);
    while (System.currentTimeMillis() < endTimeMillis) {
      outstandingTestLimiter.acquireUninterruptibly();
      rateLimiter.acquire();
      executor.submit(client).addListener(outstandingTestLimiter::release, executor);
    }
    log.info("Load test complete, shutting down.");
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

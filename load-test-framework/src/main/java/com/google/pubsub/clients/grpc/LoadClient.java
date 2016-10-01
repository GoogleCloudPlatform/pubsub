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
package com.google.pubsub.clients.grpc;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.*;
import com.google.protobuf.Empty;
import com.google.pubsub.clients.grpc.PubsubLoadClientAdapter.LoadTestParams;
import com.google.pubsub.clients.grpc.PubsubLoadClientAdapter.ProjectInfo;
import com.google.pubsub.clients.grpc.PubsubLoadClientAdapter.PublishResponseResult;
import com.google.pubsub.clients.grpc.PubsubLoadClientAdapter.PullResponseResult;
import com.google.pubsub.flic.common.Command;
import com.google.pubsub.flic.common.LoadtestFrameworkGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A high performance load test client for Cloud Pub/Sub. That supports gRPC as underlying transport methods.
 */
public class LoadClient {
  private static final Logger log = LoggerFactory.getLogger(LoadClient.class);
  private static final int REQUEST_FAILED_CODE = -1;  // A client side error occurred.
  private RateLimiter rateLimiter;
  private Semaphore concurrentLimiter;
  private ScheduledExecutorService executorService;
  private String topicName;
  private String subscriptionName;
  private MetricsHandler metricsHandler;
  private PubsubLoadClientAdapter pubsubClient;
  private String topicPath;
  private String subscriptionPath;
  private Server server;

  private LoadClient() {
  }

  public static void main(String[] args) throws Exception {
    // Hangs until done.
    new LoadClient().start();
    log.info("Closing all - good bye!");
  }

  private void start() throws Exception {
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
    if (request.hasStartTime()) {
      Preconditions.checkArgument(request.getStartTime().getSeconds() * 1000 > System.currentTimeMillis());
      Thread.sleep(request.getStartTime().getSeconds() * 1000 - System.currentTimeMillis());
    }
    final String project = Preconditions.checkNotNull(request.getProject());
    topicName = Preconditions.checkNotNull(request.getTopic());
    topicPath = "projects/" + project + "/topics/" + topicName;
    subscriptionName = Preconditions.checkNotNull(request.getSubscription());
    subscriptionPath = "projects/" + project + "/subscriptions/" + subscriptionName;
    rateLimiter = RateLimiter.create(request.getRequestRate());
    concurrentLimiter = new Semaphore(request.getNumberOfWorkers(), false);

    executorService =
        Executors.newScheduledThreadPool(
            request.getNumberOfWorkers() + 10,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("load-thread").build());

    metricsHandler = new MetricsHandler(project);
    metricsHandler.initialize();

    ProjectInfo projectInfo = new ProjectInfo(project, topicName, subscriptionName);
    LoadTestParams loadTestParams =
        new LoadTestParams(
            request.getMessageSize(),
            request.getMaxMessagesPerPull(),
            request.getNumberOfWorkers(),
            30000);

    pubsubClient = new PubsubGrpcLoadClient(projectInfo, loadTestParams);
    startLoad();

    final long endTime = request.getStopTime().getSeconds() * 1000;
    Preconditions.checkArgument(endTime < System.currentTimeMillis());
    executorService.awaitTermination(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    executorService.shutdownNow();
  }

  private void startLoad() {
    if (subscriptionName.isEmpty()) {
      executorService.submit(() -> {
        while (true) {
          rateLimiter.acquire();
          concurrentLimiter.acquireUninterruptibly();
          final Stopwatch stopWatch = Stopwatch.createStarted();
          ListenableFuture<PublishResponseResult> publishFuture =
              pubsubClient.publishMessages(topicPath);
          Futures.addCallback(
              publishFuture,
              new FutureCallback<PublishResponseResult>() {
                @Override
                public void onSuccess(PublishResponseResult result) {
                  long elapsed = stopWatch.elapsed(TimeUnit.MILLISECONDS);
                  concurrentLimiter.release();
                  if (result.isOk()) {
                    //metricsHandler.recordPublishAckLatency(elapsed);
                  }
                  /*metricsHandler.recordRequestCount(
                      topicName,
                      Operation.PUBLISH.toString(),
                      result.getStatusCode(),
                      result.getMessagesPublished());*/
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                  concurrentLimiter.release();
                  log.warn(
                      "Unable to execute a publish request (client-side error)", t);
                  //metricsHandler.recordRequestCount(topicName, Operation.PUBLISH.toString(), REQUEST_FAILED_CODE, 0);
                }
              });
        }
      });
      return;
    }
    executorService.submit(() -> {
      while (true) {
        concurrentLimiter.acquireUninterruptibly();
        rateLimiter.acquire();
        final Stopwatch stopWatch = Stopwatch.createStarted();
        ListenableFuture<PullResponseResult> pullFuture =
            pubsubClient.pullMessages(subscriptionPath);
        Futures.addCallback(
            pullFuture,
            new FutureCallback<PullResponseResult>() {
              @Override
              public void onSuccess(PullResponseResult result) {
                concurrentLimiter.release();
                recordPullResponseResult(result, stopWatch.elapsed(TimeUnit.MILLISECONDS));
              }

              @Override
              public void onFailure(@Nonnull Throwable t) {
                log.warn(
                    "Unable to execute a pull request (client-side error)", t);
                //metricsHandler.recordRequestCount(subscriptionName, Operation.PULL.toString(), REQUEST_FAILED_CODE, 0);
              }
            });
      }
    });

    metricsHandler.startReporting();
  }

  private void recordPullResponseResult(
      PullResponseResult result, long elapsed) {
    if (result.isOk()) {
      result.getEndToEndLatenciesMillis().forEach(metricsHandler::recordEndToEndLatency);
    }
    /*metricsHandler.recordRequestCount(
        topicName,
        Operation.PULL.toString(),
        result.getStatusCode(),
        result.getMessagesPulled());*/
  }

  private enum Operation {
    PUBLISH,
    PULL,
  }
}


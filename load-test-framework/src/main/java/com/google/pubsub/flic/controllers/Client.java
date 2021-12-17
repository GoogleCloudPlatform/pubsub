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

package com.google.pubsub.flic.controllers;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.pubsub.flic.common.*;
import com.google.pubsub.flic.common.LoadtestProto.PubsubOptions;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.flic.common.LoadtestProto.StartResponse;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages remote clients by starting, performing health checks, and collecting statistics on
 * running clients.
 */
public class Client {
  public static final String TOPIC = "cloud-pubsub-loadtest";
  public static final String SUBSCRIPTION = "loadtest-subscriber";
  public static final String RESOURCE_DIR = "target/classes/gce";
  private static final Logger log = LoggerFactory.getLogger(Client.class);
  private static final int DEFAULT_PORT = 5000;

  public static final int PUBLISHER_CPU_SCALING = 5;

  private final String networkAddress;
  private final ScheduledExecutorService executorService;

  private final Integer port;

  private ClientStatus clientStatus;
  private ManagedChannel channel;
  private LoadtestWorkerGrpc.LoadtestWorkerStub stub;
  private int errors = 0;
  private Duration runningDuration = Durations.fromMillis(0);
  private SettableFuture<Void> doneFuture = SettableFuture.create();

  private MessageTracker messageTracker;
  private LatencyTracker latencyTracker;

  // StartRequest options
  // General options
  private final ClientParams params;

  public Client(
      String networkAddress, ClientParams params, ScheduledExecutorService executorService) {
    this(networkAddress, params, executorService, DEFAULT_PORT);
  }

  public Client(
      String networkAddress,
      ClientParams params,
      ScheduledExecutorService executorService,
      Integer port) {
    this.networkAddress = networkAddress;
    this.clientStatus = ClientStatus.NONE;
    this.params = params;
    this.executorService = executorService;
    this.port = port;
    this.channel =
        ManagedChannelBuilder.forAddress(networkAddress, port)
            .usePlaintext()
            .maxInboundMessageSize(1000000000)
            .build();
    long startTimeMillis = System.currentTimeMillis();
    while ((System.currentTimeMillis() - startTimeMillis) < 600000) {
      ConnectivityState state = this.channel.getState(true);
      if (state == ConnectivityState.READY) {
        doneFuture.addListener(
            () -> {
              this.channel.shutdownNow();
              try {
                this.channel.awaitTermination(1, TimeUnit.MINUTES);
              } catch (InterruptedException e) {
                // Failed to shutdown the channel.  Since the worker is being destroyed, this has no
                // adverse
                // effects other than printing an error to stderr.
              }
            },
            MoreExecutors.directExecutor());
        return;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException(String.format("Unable to connect to client %s:%d in 600 seconds.",
        networkAddress, port));
  }

  ClientType getClientType() {
    return params.getClientType();
  }

  ListenableFuture<Void> getDoneFuture() {
    return doneFuture;
  }

  private LoadtestWorkerGrpc.LoadtestWorkerStub getStub() {
    return LoadtestWorkerGrpc.newStub(channel);
  }

  long getRunningSeconds() {
    return runningDuration.getSeconds();
  }

  void start(Timestamp startTime, MessageTracker messageTracker, LatencyTracker latencyTracker)
      throws Throwable {
    this.messageTracker = messageTracker;
    this.latencyTracker = latencyTracker;
    // Send a gRPC call to start the server
    log.info("Connecting to " + networkAddress + ":" + port);
    log.info("Starting at " + startTime);
    StartRequest.Builder requestBuilder =
        StartRequest.newBuilder()
            .setProject(params.getProject())
            .setTopic(TOPIC)
            .setStartTime(startTime)
            .setTestDuration(
                Durations.add(
                    params.getTestParameters().loadtestDuration(),
                    params.getTestParameters().burnInDuration()))
            .setIncludeIds(params.getTestParameters().publishRatePerSec().isPresent());
    if (params.getClientType().isPublisher()) {
      LoadtestProto.PublisherOptions.Builder publisherOptions =
          LoadtestProto.PublisherOptions.newBuilder()
              .setMessageSize(params.getTestParameters().messageSize())
              .setBatchDuration(params.getTestParameters().publishBatchDuration())
              .setBatchSize(params.getTestParameters().publishBatchSize());
      if (params.getTestParameters().publishRatePerSec().isPresent()) {
        publisherOptions.setRate(params.getTestParameters().publishRatePerSec().get());
      }
      requestBuilder.setPublisherOptions(publisherOptions);
      requestBuilder.setCpuScaling(PUBLISHER_CPU_SCALING);
    } else {
      requestBuilder.setCpuScaling(params.getTestParameters().subscriberCpuScaling());
      if (params.getClientType().isCps()) {
        requestBuilder.setPubsubOptions(PubsubOptions.newBuilder().setSubscription(SUBSCRIPTION));
      }
    }

    StartRequest request = requestBuilder.build();
    log.warn("Request to start: " + request);
    SettableFuture<Void> startFuture = SettableFuture.create();
    stub = getStub();
    stub.start(
        request,
        new StreamObserver<StartResponse>() {
          private int connectionErrors = 0;

          @Override
          public void onNext(StartResponse response) {
            log.info("Successfully started client [" + networkAddress + "]");
            clientStatus = ClientStatus.RUNNING;
            startFuture.set(null);
          }

          @Override
          public void onError(Throwable throwable) {
            if (connectionErrors > 10) {
              log.error("Client failed to start " + connectionErrors + " times, shutting down.");
              clientStatus = ClientStatus.FAILED;
              startFuture.setException(throwable);
              doneFuture.setException(throwable);
              return;
            }
            connectionErrors++;
            try {
              Thread.sleep(10000);
            } catch (InterruptedException e) {
              log.info("Interrupted during back off, retrying.");
            }
            log.debug("Going to retry client connection, likely due to start up time.");
            stub = getStub();
            stub.start(request, this);
          }

          @Override
          public void onCompleted() {}
        });
    try {
      startFuture.get();
      ScheduledFuture<?> checkFuture =
          executorService.scheduleAtFixedRate(this::checkClient, 10, 10, TimeUnit.SECONDS);
      doneFuture.addListener(() -> checkFuture.cancel(false), executorService);
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  private void checkClient() {
    log.info("Checking...");
    if (clientStatus != ClientStatus.RUNNING) {
      return;
    }
    stub.check(
        LoadtestProto.CheckRequest.getDefaultInstance(),
        new StreamObserver<LoadtestProto.CheckResponse>() {
          @Override
          public void onNext(LoadtestProto.CheckResponse checkResponse) {
            LoadtestProto.CheckResponse cloned =
                checkResponse.toBuilder().clearReceivedMessages().build();
            log.info(
                "Connected to "
                    + params.getClientType()
                    + " client.  Got check response: "
                    + cloned);
            if (checkResponse.getIsFinished()) {
              clientStatus = ClientStatus.STOPPED;
              doneFuture.set(null);
            }
            if (params.getClientType().isPublisher()) {
              messageTracker.addSent(checkResponse.getReceivedMessagesList());
            } else {
              messageTracker.addReceived(checkResponse.getReceivedMessagesList());
            }
            runningDuration = checkResponse.getRunningDuration();
            // Has been running for longer than the burn in duration.
            if (Durations.compare(runningDuration, params.getTestParameters().burnInDuration())
                > 0) {
              latencyTracker.addLatencies(checkResponse.getBucketValuesList());
              log.info(
                  "Approximate rate: "
                      + StatsUtils.getThroughput(
                          latencyTracker.getCount(),
                          Durations.subtract(
                              runningDuration, params.getTestParameters().burnInDuration()),
                          params.getTestParameters().messageSize())
                      + " MB/s");
            }
          }

          @Override
          public void onError(Throwable throwable) {
            if (errors > 3) {
              clientStatus = ClientStatus.FAILED;
              doneFuture.setException(throwable);
              log.error(
                  params.getClientType()
                      + " client failed "
                      + errors
                      + " health checks, something went wrong.");
              return;
            }
            log.warn(
                "Unable to connect to "
                    + params.getClientType()
                    + " client, probably a transient error.");
            stub = getStub();
            errors++;
          }

          @Override
          public void onCompleted() {
            errors = 0;
          }
        });
  }

  private enum ClientStatus {
    NONE,
    RUNNING,
    STOPPED,
    FAILED,
  }
}

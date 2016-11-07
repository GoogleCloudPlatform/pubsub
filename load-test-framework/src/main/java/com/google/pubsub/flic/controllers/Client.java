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

package com.google.pubsub.flic.controllers;

import com.beust.jcommander.internal.Nullable;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.pubsub.flic.common.LatencyDistribution;
import com.google.pubsub.flic.common.LoadtestGrpc;
import com.google.pubsub.flic.common.LoadtestProto;
import com.google.pubsub.flic.common.LoadtestProto.KafkaOptions;
import com.google.pubsub.flic.common.LoadtestProto.PubsubOptions;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.flic.common.LoadtestProto.StartResponse;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.rmi.UnexpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages remote clients by starting, performing health checks, and collecting statistics
 * on running clients.
 */
public class Client {
  static final String TOPIC_PREFIX = "cloud-pubsub-loadtest-";
  private static final Logger log = LoggerFactory.getLogger(Client.class);
  private static final int PORT = 5000;
  public static int messageSize;
  public static int requestRate;
  public static Timestamp startTime;
  public static int loadtestLengthSeconds;
  public static int publishBatchSize;
  public static int maxMessagesPerPull;
  public static int pollLength;
  public static String broker;
  public static int maxOutstandingRequests;
  public static long burnInTimeMillis;
  public static int numberOfMessages = 0;
  private final ClientType clientType;
  private final String networkAddress;
  private final String project;
  private final String topic;
  private final String subscription;
  private final ScheduledExecutorService executorService;
  private ClientStatus clientStatus;
  private LoadtestGrpc.LoadtestStub stub;
  private int errors = 0;
  private long[] bucketValues = new long[LatencyDistribution.LATENCY_BUCKETS.length];
  private long runningSeconds = 0;
  private SettableFuture<Void> doneFuture = SettableFuture.create();

  Client(ClientType clientType, String networkAddress, String project,
         @Nullable String subscription, ScheduledExecutorService executorService) {
    this.clientType = clientType;
    this.networkAddress = networkAddress;
    this.clientStatus = ClientStatus.NONE;
    this.project = project;
    this.topic = TOPIC_PREFIX + getTopicSuffix(clientType);
    this.subscription = subscription;
    this.executorService = executorService;
  }

  static String getTopicSuffix(ClientType clientType) {
    switch (clientType) {
      case CPS_GCLOUD_PUBLISHER:
      case CPS_GCLOUD_SUBSCRIBER:
        return "gcloud";
      case CPS_GCLOUD_PYTHON_PUBLISHER:
        return "gcloud";
      case CPS_GRPC_PUBLISHER:
      case CPS_GRPC_SUBSCRIBER:
        return "grpc";
      case KAFKA_PUBLISHER:
      case KAFKA_SUBSCRIBER:
        return "kafka";
    }
    return null;
  }

  ClientType getClientType() {
    return clientType;
  }

  ListenableFuture<Void> getDoneFuture() {
    return doneFuture;
  }

  private LoadtestGrpc.LoadtestStub getStub() {
    return LoadtestGrpc.newStub(
        ManagedChannelBuilder.forAddress(networkAddress, PORT).usePlaintext(true).build());
  }

  long getRunningSeconds() {
    return runningSeconds;
  }

  long[] getBucketValues() {
    return bucketValues;
  }

  void start() throws Throwable {
    // Send a gRPC call to start the server
    log.info("Connecting to " + networkAddress + ":" + PORT);
    StartRequest.Builder requestBuilder = StartRequest.newBuilder()
        .setProject(project)
        .setTopic(topic)
        .setMaxOutstandingRequests(maxOutstandingRequests)
        .setMessageSize(messageSize)
        .setRequestRate(requestRate)
        .setStartTime(startTime)
        .setPublishBatchSize(publishBatchSize);
    if (numberOfMessages > 0) {
      requestBuilder.setNumberOfMessages(numberOfMessages);
    } else {
      requestBuilder.setTestDuration(Duration.newBuilder()
          .setSeconds(loadtestLengthSeconds).build());
    }
    switch (clientType) {
      case CPS_GCLOUD_SUBSCRIBER:
      case CPS_GRPC_SUBSCRIBER:
        requestBuilder.setPubsubOptions(PubsubOptions.newBuilder()
            .setSubscription(subscription)
            .setMaxMessagesPerPull(maxMessagesPerPull));
        break;
      case KAFKA_PUBLISHER:
        requestBuilder.setKafkaOptions(KafkaOptions.newBuilder()
            .setBroker(broker));
        break;
      case KAFKA_SUBSCRIBER:
        requestBuilder.setKafkaOptions(KafkaOptions.newBuilder()
            .setBroker(broker)
            .setPollLength(pollLength));
        break;
    }
    StartRequest request = requestBuilder.build();
    SettableFuture<Void> startFuture = SettableFuture.create();
    stub = getStub();
    stub.start(request, new StreamObserver<StartResponse>() {
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
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          log.info("Interrupted during back off, retrying.");
        }
        log.debug("Going to retry client connection, likely due to start up time.");
        stub = getStub();
        stub.start(request, this);
      }

      @Override
      public void onCompleted() {
      }
    });
    try {
      startFuture.get();
      executorService.scheduleAtFixedRate(this::checkClient, 20, 20, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  private void checkClient() {
    if (clientStatus != ClientStatus.RUNNING) {
      return;
    }
    stub.check(LoadtestProto.CheckRequest.getDefaultInstance(),
        new StreamObserver<LoadtestProto.CheckResponse>() {
          @Override
          public void onNext(LoadtestProto.CheckResponse checkResponse) {
            log.debug("Connected to client.");
            if (checkResponse.getIsFinished()) {
              clientStatus = ClientStatus.STOPPED;
              doneFuture.set(null);
            }
            if (System.currentTimeMillis() < burnInTimeMillis) {
              return;
            }
            synchronized (this) {
              for (int i = 0; i < LatencyDistribution.LATENCY_BUCKETS.length; i++) {
                bucketValues[i] += checkResponse.getBucketValues(i);
              }
              runningSeconds = checkResponse.getRunningDuration().getSeconds();
            }
          }

          @Override
          public void onError(Throwable throwable) {
            if (errors > 3) {
              clientStatus = ClientStatus.FAILED;
              doneFuture.setException(throwable);
              log.error(clientType + " client failed " + errors +
                  " health checks, something went wrong.");
              return;
            }
            log.warn("Unable to connect to " + clientType + " client, probably a transient error.");
            stub = getStub();
            errors++;
          }

          @Override
          public void onCompleted() {
            errors = 0;
          }
        });
  }

  /**
   * An enum representing the possible client types.
   */
  public enum ClientType {
    CPS_GRPC_PUBLISHER,
    CPS_GRPC_SUBSCRIBER,
    CPS_GCLOUD_PUBLISHER,
    CPS_GCLOUD_SUBSCRIBER,
    CPS_GCLOUD_PYTHON_PUBLISHER,
    KAFKA_PUBLISHER,
    KAFKA_SUBSCRIBER;

    public boolean isCpsPublisher() {
      switch (this) {
        case CPS_GRPC_PUBLISHER:
        case CPS_GCLOUD_PUBLISHER:
        case CPS_GCLOUD_PYTHON_PUBLISHER:
          return true;
        default:
          return false;
      }
    }

    public boolean isPublisher() {
      switch (this) {
        case CPS_GCLOUD_PUBLISHER:
        case CPS_GCLOUD_PYTHON_PUBLISHER:
        case KAFKA_PUBLISHER:
        case CPS_GRPC_PUBLISHER:
          return true;
        default:
          return false;
      }
    }

    public ClientType getSubscriberType() {
      switch (this) {
        case CPS_GRPC_PUBLISHER:
          return CPS_GRPC_SUBSCRIBER;
        case CPS_GCLOUD_PUBLISHER:
        case CPS_GCLOUD_PYTHON_PUBLISHER:
          return CPS_GCLOUD_SUBSCRIBER;
        case KAFKA_PUBLISHER:
          return KAFKA_SUBSCRIBER;
        default:
          return this;
      }
    }

    public String getTypeString() throws IllegalAccessException {
      switch (this) {
        case CPS_GCLOUD_PUBLISHER:
        case CPS_GCLOUD_SUBSCRIBER:
          return "gcloud";
        case CPS_GCLOUD_PYTHON_PUBLISHER:
          return "python gcloud";
        case CPS_GRPC_SUBSCRIBER:
        case CPS_GRPC_PUBLISHER:
          return "grpc";
        case KAFKA_PUBLISHER:
        case KAFKA_SUBSCRIBER:
          return "Apache";
        default:
          throw new IllegalAccessException("No type specified for this client");
      }
    }

    @Override
    public String toString() {
      return name().toLowerCase().replace('_', '-');
    }
  }

  private enum ClientStatus {
    NONE,
    RUNNING,
    STOPPED,
    FAILED,
  }
}

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
import com.google.protobuf.util.Durations;
import com.google.pubsub.flic.common.LatencyTracker;
import com.google.pubsub.flic.common.LoadtestProto;
import com.google.pubsub.flic.common.LoadtestProto.KafkaOptions;
import com.google.pubsub.flic.common.LoadtestProto.PubsubOptions;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.flic.common.LoadtestProto.StartResponse;
import com.google.pubsub.flic.common.LoadtestWorkerGrpc;
import com.google.pubsub.flic.common.MessageTracker;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages remote clients by starting, performing health checks, and collecting statistics
 * on running clients.
 */
public class Client {
  public static final String TOPIC = "cloud-pubsub-loadtest";
  private static final Logger log = LoggerFactory.getLogger(Client.class);
  private static final int DEFAULT_PORT = 5000;

  private final ClientType clientType;
  private final String networkAddress;
  private final ScheduledExecutorService executorService;

  private final Integer port;

  private ClientStatus clientStatus;
  private LoadtestWorkerGrpc.LoadtestWorkerStub stub;
  private int errors = 0;
  private Duration runningDuration = Durations.fromMillis(0);
  private SettableFuture<Void> doneFuture = SettableFuture.create();

  private MessageTracker messageTracker;
  private LatencyTracker latencyTracker;

  public static String resourceDirectory = "target/classes/gce";

  // StartRequest options
  // General options
  private final String project;
  private final String topic;
  public static Timestamp startTime;
  public static Duration loadtestDuration;
  public static Duration burnInDuration;

  // Publisher options
  public static int messageSize;
  public static int perWorkerPublishRate;
  public static int publishBatchSize;
  public static Duration publishBatchDuration;

  // Pubsub options
  private final String subscription;

  // Kafka options
  public static Duration pollDuration;
  public static String broker;
  public static String zookeeperIpAddress;
  public static int replicationFactor;
  public static int partitions;

  Client(
          ClientType clientType,
          String networkAddress,
          String project,
          @Nullable String subscription,
          ScheduledExecutorService executorService) {
    this(clientType, networkAddress, project, subscription, executorService, DEFAULT_PORT);
  }

  public Client(
      ClientType clientType,
      String networkAddress,
      String project,
      @Nullable String subscription,
      ScheduledExecutorService executorService,
      Integer port) {
    this.clientType = clientType;
    this.networkAddress = networkAddress;
    this.clientStatus = ClientStatus.NONE;
    this.project = project;
    this.topic = TOPIC;
    this.subscription = subscription;
    this.executorService = executorService;
    this.port = port;
  }

  ClientType getClientType() {
    return clientType;
  }

  ListenableFuture<Void> getDoneFuture() {
    return doneFuture;
  }

  private LoadtestWorkerGrpc.LoadtestWorkerStub getStub() {
    return LoadtestWorkerGrpc.newStub(
        ManagedChannelBuilder.forAddress(networkAddress, port)
            .usePlaintext()
            .maxInboundMessageSize(1000000000)
            .build());
  }

  long getRunningSeconds() {
    return runningDuration.getSeconds();
  }

  void start(MessageTracker messageTracker, LatencyTracker latencyTracker) throws Throwable {
    this.messageTracker = messageTracker;
    this.latencyTracker = latencyTracker;
    // Send a gRPC call to start the server
    log.info("Connecting to " + networkAddress + ":" + port);
    log.info("Starting at " + startTime);
    StartRequest.Builder requestBuilder =
        StartRequest.newBuilder()
            .setProject(project)
            .setTopic(topic)
            .setStartTime(startTime)
            .setTestDuration(Durations.add(loadtestDuration, burnInDuration))
            .setIncludeIds(perWorkerPublishRate > 0);
    if (clientType.isPublisher()) {
      requestBuilder.setPublisherOptions(
              LoadtestProto.PublisherOptions.newBuilder()
                      .setMessageSize(messageSize)
                      .setBatchDuration(publishBatchDuration)
                      .setRate(perWorkerPublishRate)
                      .setBatchSize(publishBatchSize));
    }
    if (clientType.isCps() && !clientType.isPublisher()) {
      requestBuilder.setPubsubOptions(PubsubOptions.newBuilder().setSubscription(subscription));
    }
    if (clientType.isKafka()) {
      requestBuilder.setKafkaOptions(KafkaOptions.newBuilder()
              .setBroker(broker)
              .setPollDuration(pollDuration)
              .setZookeeperIpAddress(zookeeperIpAddress)
              .setReplicationFactor(replicationFactor)
              .setPartitions(partitions));
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
      executorService.scheduleAtFixedRate(this::checkClient, 20, 20, TimeUnit.SECONDS);
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
            LoadtestProto.CheckResponse cloned = checkResponse.toBuilder().clearReceivedMessages().build();
            log.info("Connected to " + clientType + " client.  Got check response: " + cloned);
            if (checkResponse.getIsFinished()) {
              clientStatus = ClientStatus.STOPPED;
              doneFuture.set(null);
            }
            if (clientType.isPublisher()) {
              messageTracker.addSent(checkResponse.getReceivedMessagesList());
            } else {
              messageTracker.addReceived(checkResponse.getReceivedMessagesList());
            }
            runningDuration = checkResponse.getRunningDuration();
            // Has been running for longer than the burn in duration.
            if (Durations.comparator().compare(runningDuration, burnInDuration) > 0) {
              latencyTracker.addLatencies(checkResponse.getBucketValuesList());
            }
          }

          @Override
          public void onError(Throwable throwable) {
            if (errors > 3) {
              clientStatus = ClientStatus.FAILED;
              doneFuture.setException(throwable);
              log.error(clientType + " client failed " + errors
                      + " health checks, something went wrong.");
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

  public enum MessagingType {
    CPS_GCLOUD, KAFKA
  }
  
  public enum Language {
    JAVA, PYTHON, RUBY, GO, NODE, DOTNET
  }
  
  public enum MessagingSide {
    PUBLISHER, SUBSCRIBER
  }

  /**
   * A class representing the possible client types.
   */
  public static class ClientType {
    public static ClientType CPS_GCLOUD_JAVA_PUBLISHER = new ClientType(MessagingType.CPS_GCLOUD, Language.JAVA, MessagingSide.PUBLISHER);
    public static ClientType CPS_GCLOUD_JAVA_SUBSCRIBER = new ClientType(MessagingType.CPS_GCLOUD, Language.JAVA, MessagingSide.SUBSCRIBER);
    public static ClientType CPS_GCLOUD_PYTHON_PUBLISHER = new ClientType(MessagingType.CPS_GCLOUD, Language.PYTHON, MessagingSide.PUBLISHER);
    public static ClientType CPS_GCLOUD_PYTHON_SUBSCRIBER = new ClientType(MessagingType.CPS_GCLOUD, Language.PYTHON, MessagingSide.SUBSCRIBER);
    public static ClientType CPS_GCLOUD_RUBY_PUBLISHER = new ClientType(MessagingType.CPS_GCLOUD, Language.RUBY, MessagingSide.PUBLISHER);
    public static ClientType CPS_GCLOUD_RUBY_SUBSCRIBER = new ClientType(MessagingType.CPS_GCLOUD, Language.RUBY, MessagingSide.SUBSCRIBER);
    public static ClientType CPS_GCLOUD_GO_PUBLISHER = new ClientType(MessagingType.CPS_GCLOUD, Language.GO, MessagingSide.PUBLISHER);
    public static ClientType CPS_GCLOUD_GO_SUBSCRIBER = new ClientType(MessagingType.CPS_GCLOUD, Language.GO, MessagingSide.SUBSCRIBER);
    public static ClientType CPS_GCLOUD_NODE_PUBLISHER = new ClientType(MessagingType.CPS_GCLOUD, Language.NODE, MessagingSide.PUBLISHER);
    public static ClientType CPS_GCLOUD_NODE_SUBSCRIBER = new ClientType(MessagingType.CPS_GCLOUD, Language.NODE, MessagingSide.SUBSCRIBER);
    public static ClientType CPS_GCLOUD_DOTNET_PUBLISHER = new ClientType(MessagingType.CPS_GCLOUD, Language.DOTNET, MessagingSide.PUBLISHER);
    public static ClientType CPS_GCLOUD_DOTNET_SUBSCRIBER = new ClientType(MessagingType.CPS_GCLOUD, Language.DOTNET, MessagingSide.SUBSCRIBER);
    public static ClientType KAFKA_PUBLISHER = new ClientType(MessagingType.KAFKA, Language.JAVA, MessagingSide.PUBLISHER);
    public static ClientType KAFKA_SUBSCRIBER = new ClientType(MessagingType.KAFKA, Language.JAVA, MessagingSide.SUBSCRIBER);
    
    public final MessagingType messaging;
    public final Language language;
    public final MessagingSide side;
    private ClientType(MessagingType messaging, Language language, MessagingSide side) {
      this.messaging = messaging;
      this.language = language;
      this.side = side;
    }

    public boolean isCps() {
      return messaging == MessagingType.CPS_GCLOUD;
    }

    public boolean isKafka() {
      return messaging == MessagingType.KAFKA;
    }

    public boolean isPublisher() {
      return side == MessagingSide.PUBLISHER;
    }

    @Override
    public String toString() {
      if (isKafka()) {
        return (messaging + "-" + side).toLowerCase();
      }
      return (messaging.toString().replace("_", "-") + "-" + language + "-" + side).toLowerCase();
    }
  }

  private enum ClientStatus {
    NONE,
    RUNNING,
    STOPPED,
    FAILED,
  }
}

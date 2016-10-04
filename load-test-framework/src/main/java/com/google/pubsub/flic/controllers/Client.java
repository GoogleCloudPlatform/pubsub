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
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.pubsub.flic.common.Command;
import com.google.pubsub.flic.common.LoadtestFrameworkGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class Client {
  static final String topicPrefix = "cloud-pubsub-loadtest-";
  private static final Logger log = LoggerFactory.getLogger(Client.class.getName());
  private static final int port = 5000;
  public static int messageSize;
  public static int requestRate;
  public static Timestamp startTime;
  public static int loadtestLengthSeconds;
  public static int batchSize;
  private final ClientType clientType;
  private String networkAddress;
  private ClientStatus clientStatus;
  private String project;
  private String topic;
  private String subscription;

  Client(ClientType clientType, String networkAddress, String project, @Nullable String subscription) {
    this.clientType = clientType;
    this.networkAddress = networkAddress;
    this.clientStatus = ClientStatus.NONE;
    this.project = project;
    this.topic = topicPrefix + getTopicSuffix(clientType);
    this.subscription = subscription;
  }

  public static String getTopicSuffix(ClientType clientType) {
    switch (clientType) {
      case CPS_GRPC_PUBLISHER:
      case CPS_GRPC_SUBSCRIBER:
        return "cps";
      case KAFKA_PUBLISHER:
      case KAFKA_SUBSCRIBER:
        return "kafka";
    }
    return null;
  }

  public ClientStatus clientStatus() {
    return clientStatus;
  }

  ClientType clientType() {
    return clientType;
  }

  public void setNetworkAddress(String networkAddress) {
    this.networkAddress = networkAddress;
  }

  LoadtestFrameworkGrpc.LoadtestFrameworkStub getStub() {
    return LoadtestFrameworkGrpc.newStub(
        ManagedChannelBuilder.forAddress(networkAddress, port).usePlaintext(true).build());
  }

  void start() throws Throwable {
    // Send a gRPC call to start the server
    log.info("Connecting to " + networkAddress + ":" + port);
    ManagedChannel channel = ManagedChannelBuilder.forAddress(networkAddress, port).usePlaintext(true).build();

    LoadtestFrameworkGrpc.LoadtestFrameworkStub stub = LoadtestFrameworkGrpc.newStub(channel);
    Command.CommandRequest.Builder requestBuilder = Command.CommandRequest.newBuilder()
        .setProject(project)
        .setTopic(topic)
        .setMaxMessagesPerPull(batchSize)
        .setNumberOfWorkers(10)
        .setMessageSize(messageSize)
        .setRequestRate(5)
        //.setStartTime(startTime)
        .setStopTime(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000 +
            loadtestLengthSeconds).build());
    if (clientType.toString().contains("subscriber")) {
      requestBuilder.setSubscription(subscription);
    }
    Command.CommandRequest request = requestBuilder.build();
    SettableFuture<Void> startFuture = SettableFuture.create();
    getStub().startClient(request, new StreamObserver<Empty>() {
      private int connectionErrors = 0;
      @Override
      public void onNext(Empty empty) {
        log.info("Successfully started client [" + networkAddress + "]");
        clientStatus = ClientStatus.RUNNING;
      }

      @Override
      public void onError(Throwable throwable) {
        log.error("Unable to start client [" + networkAddress + "]", throwable);
        clientStatus = ClientStatus.FAILED;
        if (connectionErrors > 10) {
          log.error("Client failed " + connectionErrors + " times, shutting down.");
          startFuture.setException(throwable);
          return;
        }
        connectionErrors++;
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          log.info("Interrupted during back off, retrying.");
        }
        log.info("Going to retry client connection, likely due to start up time.");
        getStub().startClient(request, this);
      }

      @Override
      public void onCompleted() {
        log.info("Start command completed.");
        startFuture.set(null);
      }
    });
    try {
      startFuture.get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  public enum ClientType {
    CPS_GRPC_PUBLISHER,
    CPS_GRPC_SUBSCRIBER,
    KAFKA_PUBLISHER,
    KAFKA_SUBSCRIBER;

    @Override
    public String toString() {
      return name().toLowerCase().replace('_', '-');
    }
  }

  private enum ClientStatus {
    NONE,
    RUNNING,
    STOPPING,
    FAILED,
  }
}

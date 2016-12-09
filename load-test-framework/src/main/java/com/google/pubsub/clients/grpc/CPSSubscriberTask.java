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

import static com.google.pubsub.flic.controllers.Client.maxOutstandingRequests;

import com.beust.jcommander.JCommander;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Runs a task that consumes messages from a Cloud Pub/Sub subscription.
 */
class CPSSubscriberTask extends Task {
  private static final Logger log = LoggerFactory.getLogger(CPSSubscriberTask.class);
  private final String subscription;
  private final int pullSize;
  private GoogleCredentials credentials = null;
  private SubscriberGrpc.SubscriberFutureStub[] stubs;
  private int currentStubIdx;

  private CPSSubscriberTask(StartRequest request) {
    super(request, "grpc", MetricsHandler.MetricName.END_TO_END_LATENCY);
    this.subscription = "projects/"
        + request.getProject()
        + "/subscriptions/"
        + request.getSubscription();
    this.pullSize = request.getMaxMessagesPerPull();
    try {
      this.credentials =
          GoogleCredentials.getApplicationDefault()
              .createScoped(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"));
      stubs = new SubscriberGrpc.SubscriberFutureStub[request.getMaxOutstandingRequests()];
      for (int i = 0; i < stubs.length; i++) {
        stubs[i] = SubscriberGrpc.newFutureStub(getChannel());
      }
    } catch (IOException e) {
      log.error("Unable to get credentials or create channel.", e);
    }
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, CPSSubscriberTask::new);
  }

  private Channel getChannel() throws SSLException {
    return ClientInterceptors.intercept(
        NettyChannelBuilder.forAddress("pubsub.googleapis.com", 443)
            .maxMessageSize(20000000) // 20 MB
            .sslContext(GrpcSslContexts.forClient().ciphers(null).build())
            .negotiationType(NegotiationType.TLS)
            .build(),
        new ClientAuthInterceptor(credentials, Executors.newSingleThreadExecutor()));
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    SubscriberGrpc.SubscriberFutureStub stub = getStub();
    ListenableFuture<PullResponse> responseFuture = stub.pull(
        PullRequest.newBuilder()
            .setSubscription(subscription)
            .setMaxMessages(pullSize)
            .build());
    Function<PullResponse, RunResult> callback = (response) -> {
      RunResult out = new RunResult();
      List<String> ackIds = new ArrayList<>(response.getReceivedMessagesCount());
      List<ReceivedMessage> receivedList = response.getReceivedMessagesList();
      for (ReceivedMessage message : receivedList) {
        ackIds.add(message.getAckId());
        Map<String, String> attributesMap = message.getMessage().getAttributesMap();
        out.addMessageLatency(
            Integer.parseInt(attributesMap.get("clientId")),
            Integer.parseInt(attributesMap.get("sequenceNumber")),
            System.currentTimeMillis() - Long.parseLong(attributesMap.get("sendTime")));
      }
      return out;
    };
    return Futures.transform(responseFuture, callback);
  }

  private synchronized SubscriberGrpc.SubscriberFutureStub getStub() {
    SubscriberGrpc.SubscriberFutureStub stub = stubs[currentStubIdx];
    currentStubIdx = (currentStubIdx + 1) % stubs.length;
    return stub;
  }
}

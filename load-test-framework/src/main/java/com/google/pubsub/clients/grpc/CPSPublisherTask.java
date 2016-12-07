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

import static com.google.pubsub.clients.common.Task.RunResult.fromBatchSize;
import static com.google.pubsub.flic.controllers.Client.maxOutstandingRequests;
import static com.google.pubsub.flic.controllers.Client.messageSize;

import com.beust.jcommander.JCommander;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Runs a task that consumes messages from a Cloud Pub/Sub subscription.
 */
class CPSPublisherTask extends Task {
  private static final Logger log = LoggerFactory.getLogger(CPSPublisherTask.class);
  private final String topic;
  private final int batchSize;
  private final ByteString payload;
  private GoogleCredentials credentials = null;
  private PublisherGrpc.PublisherFutureStub[] stubs;
  private int currentStubIdx;
  private final Integer id;
  private final AtomicInteger sequenceNumber = new AtomicInteger(0);

  private CPSPublisherTask(StartRequest request) {
    super(request, "grpc", MetricsHandler.MetricName.PUBLISH_ACK_LATENCY);
    this.topic = "projects/"
        + request.getProject()
        + "/subscriptions/"
        + request.getSubscription();
    this.batchSize = request.getPublishBatchSize();
    this.payload = ByteString.copyFromUtf8(LoadTestRunner.createMessage(messageSize));
    try {
      this.credentials =
          GoogleCredentials.getApplicationDefault()
              .createScoped(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"));
      stubs = new PublisherGrpc.PublisherFutureStub[maxOutstandingRequests];
      for (int i = 0; i < stubs.length; i++) {
        stubs[i] = PublisherGrpc.newFutureStub(getChannel());
      }
    } catch (IOException e) {
      log.error("Unable to get credentials or create channel.", e);
    }
    this.id = (new Random()).nextInt();
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, CPSPublisherTask::new);
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

  private synchronized PublisherGrpc.PublisherFutureStub getStub() {
    PublisherGrpc.PublisherFutureStub stub = stubs[currentStubIdx];
    currentStubIdx = (currentStubIdx + 1) % stubs.length;
    return stub;
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    PublisherGrpc.PublisherFutureStub stub = getStub();
    PublishRequest.Builder requestBuilder = PublishRequest.newBuilder().setTopic(topic);
    String sendTime = String.valueOf(System.currentTimeMillis());
    for (int i = 0; i < batchSize; i++) {
      requestBuilder.addMessages(PubsubMessage.newBuilder()
          .setData(payload)
          .putAttributes("sendTime", sendTime)
          .putAttributes("clientId", id.toString())
          .putAttributes("sequenceNumber", Integer.toString(sequenceNumber.getAndIncrement())));
    }
    ListenableFuture<PublishResponse> responseFuture = stub.publish(requestBuilder.build());
    Function<PublishResponse, RunResult> callback =
        (response) -> RunResult.fromBatchSize(batchSize);
    return Futures.transform(responseFuture, callback);
  }
}

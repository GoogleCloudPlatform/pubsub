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

import com.beust.jcommander.JCommander;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
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
  private PublisherGrpc.PublisherBlockingStub[] stubs;
  private int currentStubIdx;

  private CPSPublisherTask(String project, String topic, int messageSize, int batchSize,
                           int maxOutstandingRequests) {
    super(project, "grpc", MetricsHandler.MetricName.PUBLISH_ACK_LATENCY);
    this.topic = "projects/" + project + "/topics/" + topic;
    this.batchSize = batchSize;
    this.payload = ByteString.copyFromUtf8(LoadTestRunner.createMessage(messageSize));
    try {
      this.credentials =
          GoogleCredentials.getApplicationDefault()
              .createScoped(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"));
      stubs = new PublisherGrpc.PublisherBlockingStub[maxOutstandingRequests];
      for (int i = 0; i < stubs.length; i++) {
        stubs[i] = PublisherGrpc.newBlockingStub(getChannel());
      }
    } catch (IOException e) {
      log.error("Unable to get credentials or create channel.", e);
    }
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, request ->
        new CPSPublisherTask(request.getProject(), request.getTopic(), request.getMessageSize(),
            request.getPublishBatchSize(), request.getMaxOutstandingRequests()));
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

  private synchronized PublisherGrpc.PublisherBlockingStub getStub() {
    PublisherGrpc.PublisherBlockingStub stub = stubs[currentStubIdx];
    currentStubIdx = (currentStubIdx + 1) % stubs.length;
    return stub;
  }

  @Override
  public void run() {
    PublisherGrpc.PublisherBlockingStub stub = getStub();
    PublishRequest.Builder requestBuilder = PublishRequest.newBuilder().setTopic(topic);
    String sendTime = String.valueOf(System.currentTimeMillis());
    Stopwatch stopwatch = Stopwatch.createStarted();
    for (int i = 0; i < batchSize; i++) {
      requestBuilder.addMessages(PubsubMessage.newBuilder()
          .setData(payload)
          .putAttributes("sendTime", sendTime));
    }
    PublishRequest request = requestBuilder.build();
    stub.publish(request);
    stopwatch.stop();
    addNumberOfMessages(batchSize);
    metricsHandler.recordLatencyBatch(stopwatch.elapsed(TimeUnit.MILLISECONDS), batchSize);
  }
}

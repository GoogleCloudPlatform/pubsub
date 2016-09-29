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
package com.google.pubsub.clients;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import io.grpc.Channel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

class PubsubGrpcLoadClient extends PubsubLoadClientAdapter {
  private static final Logger log = LoggerFactory.getLogger(PubsubGrpcLoadClient.class.getName());

  private final LoadTestParams loadTestParams;
  private final ProjectInfo projectInfo;
  private final String topicsBasePath;
  private final String subscriptionsBasePath;
  private final PublisherGrpc.PublisherFutureStub[] publisherFutureStubs;
  private final SubscriberGrpc.SubscriberFutureStub[] subscriberFutureStubs;
  private final String basePayloadData;
  private int currentPublisherStubIdx;
  private int currentSubscriberStubIdx;

  PubsubGrpcLoadClient(
      AccessTokenProvider accessTokenProvider,
      ProjectInfo projectInfo,
      LoadTestParams loadTestParams) {
    this.loadTestParams = loadTestParams;
    this.projectInfo = projectInfo;
    topicsBasePath = "projects/" + projectInfo.getProject() + "/topics/";
    subscriptionsBasePath = "projects/" + projectInfo.getProject() + "/subscriptions/";
    int publisherStubsCount = Math.max(1, loadTestParams.getConcurrentPublishRequests());
    publisherFutureStubs = new PublisherGrpc.PublisherFutureStub[publisherStubsCount];
    int subscriberStubsCount = Math.max(1, loadTestParams.getConcurrentPullRequests());
    subscriberFutureStubs = new SubscriberGrpc.SubscriberFutureStub[subscriberStubsCount];
    log.info("Creating %d publisher stubs.", publisherStubsCount);
    for (int i = 0; i < publisherStubsCount; ++i) {
      publisherFutureStubs[i] = PublisherGrpc.newFutureStub(getChannelGrpc())
          .withCallCredentials(MoreCallCredentials.from(accessTokenProvider.getCredentials()));
    }
    log.info("Creating %d subscriber stubs.", subscriberStubsCount);
    for (int i = 0; i < subscriberStubsCount; ++i) {
      subscriberFutureStubs[i] = SubscriberGrpc.newFutureStub(getChannelGrpc())
          .withCallCredentials(MoreCallCredentials.from(accessTokenProvider.getCredentials()));
    }
    byte[] chars = new byte[loadTestParams.getMessageSize()];
    Arrays.fill(chars, (byte) 'A');
    basePayloadData = new String(chars, Charset.forName("UTF-8"));
  }

  private String checkResourceName(String name) {
    Preconditions.checkArgument(!name.matches("[\"/]"));
    return name;
  }

  private <T extends Object> ListenableFuture<RequestResult> handleCreateFuture(
      ListenableFuture<T> future) {
    final SettableFuture<RequestResult> resultFuture = SettableFuture.create();
    Futures.addCallback(
        future,
        new FutureCallback<T>() {
          @Override
          public void onSuccess(T result) {
            resultFuture.set(new RequestResult(true, Code.OK.value()));
          }

          @Override
          public void onFailure(Throwable t) {
            if (t instanceof io.grpc.StatusRuntimeException) {
              StatusRuntimeException grpcException = (io.grpc.StatusRuntimeException) t;
              resultFuture.set(
                  new RequestResult(false, grpcException.getStatus().getCode().value()));
              return;
            }
            resultFuture.setException(t);
          }
        });
    return resultFuture;
  }

  private PublisherGrpc.PublisherFutureStub getPublisherStub() {
    PublisherGrpc.PublisherFutureStub stub = publisherFutureStubs[currentPublisherStubIdx];
    currentPublisherStubIdx = ++currentPublisherStubIdx % publisherFutureStubs.length;
    return stub.withDeadlineAfter(loadTestParams.getRequestDeadlineMillis(), TimeUnit.MILLISECONDS);
  }

  private SubscriberGrpc.SubscriberFutureStub getSubscriberStub() {
    SubscriberGrpc.SubscriberFutureStub stub = subscriberFutureStubs[currentSubscriberStubIdx];
    currentSubscriberStubIdx = ++currentSubscriberStubIdx % subscriberFutureStubs.length;
    return stub.withDeadlineAfter(loadTestParams.getRequestDeadlineMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public ListenableFuture<RequestResult> createTopic(String topicName) {
    ListenableFuture<Topic> createFuture = getPublisherStub().createTopic(
        Topic.newBuilder().setName(topicsBasePath + checkResourceName(topicName)).build());
    return handleCreateFuture(createFuture);
  }

  @Override
  public ListenableFuture<RequestResult> createSubscription(
      String subscriptionName, String topicPath) {
    ListenableFuture<Subscription> createFuture =
        getSubscriberStub().createSubscription(
            Subscription.newBuilder()
                .setName(subscriptionsBasePath + checkResourceName(subscriptionName))
                .setTopic(topicPath)
                .build());
    return handleCreateFuture(createFuture);
  }

  @Override
  public ListenableFuture<PublishResponseResult> publishMessages(String topicPath) {
    ListenableFuture<PublishResponse> publishFuture = getPublisherStub().publish(
        buildPublishRequestGrpc());
    final SettableFuture<PublishResponseResult> resultFuture = SettableFuture.create();
    Futures.addCallback(
        publishFuture,
        new FutureCallback<PublishResponse>() {
          @Override
          public void onSuccess(PublishResponse result) {
            resultFuture.set(
                new PublishResponseResult(
                    true, Code.OK.value(), loadTestParams.getPublishBatchSize()));
          }

          @Override
          public void onFailure(Throwable t) {
            if (t instanceof io.grpc.StatusRuntimeException) {
              StatusRuntimeException grpcException = (io.grpc.StatusRuntimeException) t;
              log.warn(
                  "Failed to publish, status: %s.", grpcException.getStatus(), t);
              resultFuture.set(
                  new PublishResponseResult(false, grpcException.getStatus().getCode().value(), 0));
              return;
            }
            log.warn(
                "Failed to publish, with unknown exception.", t);
            resultFuture.setException(t);
          }
        });
    return resultFuture;
  }

  @Override
  public ListenableFuture<PullResponseResult> pullMessages(String subscriptionPath) {
    ListenableFuture<PullResponse> pullFuture =
        getSubscriberStub().pull(
            PullRequest.newBuilder()
                .setSubscription(
                    "projects/"
                        + projectInfo.getProject()
                        + "/subscriptions/"
                        + projectInfo.getSubscription())
                .setMaxMessages(loadTestParams.getPullBatchSize())
                .build());
    final SettableFuture<PullResponseResult> resultFuture = SettableFuture.create();
    Futures.addCallback(
        pullFuture,
        new FutureCallback<PullResponse>() {
          @Override
          public void onSuccess(PullResponse response) {
            long now = System.currentTimeMillis();
            List<String> ackIds = new ArrayList<>();
            List<Long> endToEndLatencies = new ArrayList<>();
            for (ReceivedMessage recvMsg : response.getReceivedMessagesList()) {
              ackIds.add(recvMsg.getAckId());
              endToEndLatencies.add(
                  now - Long.parseLong(recvMsg.getMessage().getAttributes().get("sendTime")));
              log.debug("Received ackId: %s", recvMsg.getAckId());
            }
            log.debug("Received ackId count: %s", response.getReceivedMessagesCount());
            getSubscriberStub().acknowledge(
                AcknowledgeRequest.newBuilder()
                    .setSubscription(subscriptionsBasePath + projectInfo.getSubscription())
                    .addAllAckIds(ackIds)
                    .build());

            resultFuture.set(new PullResponseResult(true, 0, ackIds.size(), endToEndLatencies));
          }

          @Override
          public void onFailure(Throwable t) {
            if (t instanceof io.grpc.StatusRuntimeException) {
              StatusRuntimeException grpcException = (io.grpc.StatusRuntimeException) t;
              log.warn(
                  "Failed to pull, status: %s.", grpcException.getStatus(), t);
              resultFuture.set(
                  new PullResponseResult(
                      false,
                      grpcException.getStatus().getCode().value(),
                      0,
                      ImmutableList.of()));
              return;
            }
            log.warn(
                "Failed to pull, with unknown exception.", t);
            resultFuture.setException(t);
          }
        });
    return resultFuture;
  }

  private PublishRequest buildPublishRequestGrpc() {
    int sequence = getNextMessageId(loadTestParams.getPublishBatchSize());
    String threadName = Thread.currentThread().getName();
    long sendTime = System.currentTimeMillis();
    List<PubsubMessage> pubMessages = new ArrayList<>();
    for (int i = sequence; i < sequence + loadTestParams.getPublishBatchSize(); i++) {
      pubMessages.add(PubsubMessage.newBuilder()
          .setData(ByteString.copyFrom(basePayloadData.getBytes(Charset.forName("UTF-8"))))
          .putAllAttributes(ImmutableMap.of(
              "unique", threadName + i,
              "sendTime", String.valueOf(sendTime)))
          .build());
    }

    return PublishRequest.newBuilder()
        .setTopic(topicsBasePath + projectInfo.getTopic())
        .addAllMessages(pubMessages)
        .build();
  }

  private Channel getChannelGrpc() {
    try {
      return
          NettyChannelBuilder.forAddress(loadTestParams.getHostname(), 443)
              .maxMessageSize(20000000) // 20 MB
              .flowControlWindow(20000000) // 20 MB
              .sslContext(GrpcSslContexts.forClient().ciphers(null).build())
              .negotiationType(NegotiationType.TLS)
              .build();
    } catch (Exception e) {
      log.warn("Unable to build Channel", e);
      return null;
    }
  }
}

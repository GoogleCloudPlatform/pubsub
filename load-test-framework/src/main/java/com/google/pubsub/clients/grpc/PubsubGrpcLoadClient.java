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

import com.google.auth.oauth2.GoogleCredentials;
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

import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;
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
      ProjectInfo projectInfo,
      LoadTestParams loadTestParams) throws Exception {
    this.loadTestParams = loadTestParams;
    this.projectInfo = projectInfo;
    topicsBasePath = "projects/" + projectInfo.getProject() + "/topics/";
    subscriptionsBasePath = "projects/" + projectInfo.getProject() + "/subscriptions/";
    int publisherStubsCount = Math.max(1, loadTestParams.getConcurrentRequests());
    publisherFutureStubs = new PublisherGrpc.PublisherFutureStub[publisherStubsCount];
    int subscriberStubsCount = Math.max(1, loadTestParams.getConcurrentRequests());
    subscriberFutureStubs = new SubscriberGrpc.SubscriberFutureStub[subscriberStubsCount];
    log.info("Creating " + publisherStubsCount + " publisher stubs.", publisherStubsCount);
    GoogleCredentials credentials =
        GoogleCredentials.getApplicationDefault()
            .createScoped(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"));
    for (int i = 0; i < publisherStubsCount; ++i) {
      publisherFutureStubs[i] = PublisherGrpc.newFutureStub(getChannelGrpc())
          .withCallCredentials(MoreCallCredentials.from(credentials));
    }
    log.info("Creating " + subscriberStubsCount + " subscriber stubs.");
    for (int i = 0; i < subscriberStubsCount; ++i) {
      subscriberFutureStubs[i] = SubscriberGrpc.newFutureStub(getChannelGrpc())
          .withCallCredentials(MoreCallCredentials.from(credentials));
    }
    byte[] chars = new byte[loadTestParams.getMessageSize()];
    Arrays.fill(chars, (byte) 'A');
    basePayloadData = new String(chars, Charset.forName("UTF-8"));
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
                    true, Code.OK.value(), loadTestParams.getBatchSize()));
          }

          @Override
          public void onFailure(@Nonnull Throwable t) {
            if (t instanceof io.grpc.StatusRuntimeException) {
              StatusRuntimeException grpcException = (io.grpc.StatusRuntimeException) t;
              log.warn(
                  "Failed to publish, status: " + grpcException.getStatus() + ".", t);
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
                .setMaxMessages(loadTestParams.getBatchSize())
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
                  now - Long.parseLong(recvMsg.getMessage().getAttributesMap().get("sendTime")));
              log.debug("Received ackId: " + recvMsg.getAckId());
            }
            log.debug("Received ackId count: " + response.getReceivedMessagesCount());
            getSubscriberStub().acknowledge(
                AcknowledgeRequest.newBuilder()
                    .setSubscription(subscriptionsBasePath + projectInfo.getSubscription())
                    .addAllAckIds(ackIds)
                    .build());

            resultFuture.set(new PullResponseResult(true, 0, ackIds.size(), endToEndLatencies));
          }

          @Override
          public void onFailure(@Nonnull Throwable t) {
            if (t instanceof io.grpc.StatusRuntimeException) {
              StatusRuntimeException grpcException = (io.grpc.StatusRuntimeException) t;
              log.warn(
                  "Failed to pull, status: " + grpcException.getStatus(), t);
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
    int sequence = getNextMessageId(loadTestParams.getBatchSize());
    String threadName = Thread.currentThread().getName();
    long sendTime = System.currentTimeMillis();
    List<PubsubMessage> pubMessages = new ArrayList<>();
    for (int i = sequence; i < sequence + loadTestParams.getBatchSize(); i++) {
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

  private Channel getChannelGrpc() throws SSLException {
    return
        NettyChannelBuilder.forAddress("pubsub.googleapis.com", 443)
            .maxMessageSize(20000000) // 20 MB
            .sslContext(GrpcSslContexts.forClient().ciphers(null).build())
            .negotiationType(NegotiationType.TLS)
            .build();
  }
}

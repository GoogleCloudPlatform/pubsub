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
package com.google.pubsub.kafka.source;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Empty;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link CloudPubSubSubscriber} that uses <a href="http://www.grpc.io/">gRPC</a> to pull messages
 * from <a href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>. This class is not
 * thread-safe.
 */
public class CloudPubSubGRPCSubscriber implements CloudPubSubSubscriber {

  private static final Logger log = LoggerFactory.getLogger(CloudPubSubGRPCSubscriber.class);
  private long nextSubscriberResetTime = 0;
  private GrpcSubscriberStub subscriber;
  private final CredentialsProvider gcpCredentialsProvider;
  private final String endpoint;
  private final ProjectSubscriptionName subscriptionName;
  private final int cpsMaxBatchSize;

  CloudPubSubGRPCSubscriber(CredentialsProvider gcpCredentialsProvider, String endpoint,
      ProjectSubscriptionName subscriptionName, int cpsMaxBatchSize) {
    this.gcpCredentialsProvider = gcpCredentialsProvider;
    this.endpoint = endpoint;
    this.subscriptionName = subscriptionName;
    this.cpsMaxBatchSize = cpsMaxBatchSize;
    makeSubscriber();
  }

  @Override
  public ApiFuture<List<ReceivedMessage>> pull() {
    if (System.currentTimeMillis() > nextSubscriberResetTime) {
      makeSubscriber();
    }
    return ApiFutures.transform(subscriber.pullCallable().futureCall(
        PullRequest.newBuilder().setSubscription(subscriptionName.toString())
            .setMaxMessages(cpsMaxBatchSize).build()), PullResponse::getReceivedMessagesList,
        MoreExecutors.directExecutor());
  }

  @Override
  public ApiFuture<Empty> ackMessages(Collection<String> ackIds) {
    if (System.currentTimeMillis() > nextSubscriberResetTime) {
      makeSubscriber();
    }
    return subscriber.acknowledgeCallable().futureCall(
        AcknowledgeRequest.newBuilder().setSubscription(subscriptionName.toString())
            .addAllAckIds(ackIds).build());
  }

  @Override
  public void close() {
    subscriber.close();
  }

  private void makeSubscriber() {
    try {
      if (subscriber != null) {
        subscriber.close();
      }
      log.info("Creating subscriber.");
      SubscriberStubSettings subscriberStubSettings =
          SubscriberStubSettings.newBuilder()
              .setTransportChannelProvider(
                  SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                      .setMaxInboundMessageSize(20 << 20) // 20MB
                      .build())
              .setCredentialsProvider(gcpCredentialsProvider)
              .setEndpoint(endpoint)
              .build();
      subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
      // We change the subscriber every 25 - 35 minutes in order to avoid GOAWAY errors.
      nextSubscriberResetTime =
          System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(10 * 60 * 1000)
              + 25 * 60 * 1000;
    } catch (IOException e) {
      throw new RuntimeException("Could not create subscriber stub; no subscribing can occur.", e);
    }
  }
}

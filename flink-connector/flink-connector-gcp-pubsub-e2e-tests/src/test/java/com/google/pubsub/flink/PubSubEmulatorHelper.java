/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.pubsub.flink;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannel;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

/** Helper for local E2E testing against a Cloud Pub/Sub emulator. */
public final class PubSubEmulatorHelper {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubEmulatorHelper.class);

  private static PubSubEmulatorContainer container;

  private static TransportChannel channel;
  private static TransportChannelProvider channelProvider;

  private static TopicAdminClient topicAdminClient;
  private static SubscriptionAdminClient subscriptionAdminClient;

  private PubSubEmulatorHelper() {}

  public static String getEmulatorEndpoint() {
    String hostport = System.getenv("PUBSUB_EMULATOR_HOST");
    if (hostport != null) {
      return hostport;
    }

    if (container == null) {
      container =
          new PubSubEmulatorContainer(
              DockerImageName.parse(DockerImageVersions.GOOGLE_CLOUD_PUBSUB_EMULATOR));
      container.start();
    }
    return container.getEmulatorEndpoint();
  }

  public static TransportChannelProvider getTransportChannelProvider()
      throws IllegalStateException {
    if (channel == null) {
      channel =
          GrpcTransportChannel.create(
              ManagedChannelBuilder.forTarget(getEmulatorEndpoint()).usePlaintext().build());
    }
    if (channelProvider == null) {
      channelProvider = FixedTransportChannelProvider.create(channel);
    }
    return channelProvider;
  }

  public static CredentialsProvider getCredentialsProvider() {
    return NoCredentialsProvider.create();
  }

  public static Topic createTopic(TopicName topic) throws IOException {
    deleteTopic(topic);
    LOG.info("CreateTopic {}", topic);
    return getTopicAdminClient().createTopic(topic);
  }

  public static void deleteTopic(TopicName topic) throws IOException {
    try {
      getTopicAdminClient().getTopic(topic);
    } catch (NotFoundException e) {
      return;
    }

    LOG.info("DeleteTopic {}", topic);
    getTopicAdminClient().deleteTopic(topic);
  }

  public static Subscription createSubscription(SubscriptionName subscription, TopicName topic)
      throws IOException {
    deleteSubscription(subscription);
    LOG.info("CreateSubscription {} on topic {}", subscription, topic);
    return getSubscriptionAdminClient()
        .createSubscription(
            subscription, topic, PushConfig.getDefaultInstance(), /* ackDeadlineSeconds= */ 10);
  }

  public static void deleteSubscription(SubscriptionName subscription) throws IOException {
    try {
      getSubscriptionAdminClient().getSubscription(subscription);
    } catch (NotFoundException e) {
      return;
    }

    LOG.info("DeleteSubscription {}", subscription);
    getSubscriptionAdminClient().deleteSubscription(subscription);
  }

  public static void publishMessages(TopicName topic, List<String> payloads)
      throws ExecutionException, InterruptedException, IOException {
    Publisher publisher =
        Publisher.newBuilder(topic)
            .setChannelProvider(getTransportChannelProvider())
            .setCredentialsProvider(getCredentialsProvider())
            .build();
    for (final String payload : payloads) {
      publisher
          .publish(PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(payload)).build())
          .get();
    }
    publisher.shutdown();
    publisher.awaitTermination(1, TimeUnit.MINUTES);
  }

  public static List<PubsubMessage> pullAndAckMessages(
      SubscriptionName subscription, Integer expectedMessageCount, Integer deadlineSeconds)
      throws ExecutionException, InterruptedException, IOException {
    List<PubsubMessage> messages = new ArrayList<PubsubMessage>();
    MessageReceiver receiver =
        (PubsubMessage message, AckReplyConsumer consumer) -> {
          messages.add(message);
          consumer.ack();
        };
    Subscriber subscriber =
        Subscriber.newBuilder(
                ProjectSubscriptionName.of(
                    subscription.getProject(), subscription.getSubscription()),
                receiver)
            .setChannelProvider(getTransportChannelProvider())
            .setCredentialsProvider(getCredentialsProvider())
            .build();
    subscriber.startAsync().awaitRunning();
    for (int i = deadlineSeconds; i > 0; i--) {
      Thread.sleep(1000);
      if (messages.size() >= expectedMessageCount) {
        break;
      }
    }
    subscriber.stopAsync().awaitTerminated();
    return messages;
  }

  private static TopicAdminClient getTopicAdminClient() throws IOException {
    if (topicAdminClient == null) {
      topicAdminClient =
          TopicAdminClient.create(
              TopicAdminSettings.newBuilder()
                  .setTransportChannelProvider(getTransportChannelProvider())
                  .setCredentialsProvider(getCredentialsProvider())
                  .build());
    }
    return topicAdminClient;
  }

  private static SubscriptionAdminClient getSubscriptionAdminClient() throws IOException {
    if (subscriptionAdminClient == null) {
      subscriptionAdminClient =
          SubscriptionAdminClient.create(
              SubscriptionAdminSettings.newBuilder()
                  .setTransportChannelProvider(getTransportChannelProvider())
                  .setCredentialsProvider(getCredentialsProvider())
                  .build());
    }
    return subscriptionAdminClient;
  }
}

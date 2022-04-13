// Copyright 2020 Google Inc.
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
package com.google.cloud.pubsub.prober;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generate load on a single topic and a single subscription with a configurable number of
 * subscriber clients. Can be extended by overriding updateTopicBuilder, updateSubscriptionBuilder,
 * updatePublisherBuilder, updateSubscriberBuilder, processMessage and updateNextMessage.
 */
public class ProberStarter {
  public static class Args {
    @Parameter(
        names = "--project",
        required = true,
        description = "Cloud Project identifier to use")
    private String project = null;

    @Parameter(names = "--endpoint", description = "Cloud Pub/Sub endpoint to run against.")
    private String endpoint = "pubsub.googleapis.com:443";

    @Parameter(names = "--should_cleanup", description ="Whether the prober should start by cleaning up the topic and subscription.")
    private boolean shouldCleanup = true;

    @Parameter(names = "--no_publish", description ="Whether the prober should skip publishing.")
    private boolean noPublish = false;

    @Parameter(names = "--topic_name", description = "Name of topic to create and use for tests.")
    private String topicName = "cloud-pubsub-client-library-prober";

    @Parameter(
        names = "--subscription_name",
        description = "Name of subscription to create and use for tests.")
    private String subscriptionName = "cloud-pubsub-client-library-prober-sub";

    @Parameter(
        names = "--subscription_type",
        description = "The type of subscription and subscribers to create.")
    private Prober.SubscriptionType subscriptionType = Prober.SubscriptionType.STREAMING_PULL;

    @Parameter(
        names = "--message_failure_probability",
        description =
            "Probability of a message being NACK-ed. Valid values are from 0.0 to 1.0 inclusive.")
    private Double messageFailureProbability = 0.0;

    @Parameter(
        names = "--publish_frequency",
        description = "Time between publishes, specified in microseconds.")
    private Long publishFrequency = 1_000_000L;

    @Parameter(
        names = "--ack_delay_milliseconds",
        description = "The amount of time to wait to ack messages in milliseconds.")
    private Integer ackDelayMilliseconds = 0;

    @Parameter(
        names = "--ack_deadline_seconds",
        description = "The ack deadline to set on the subscription in seconds.")
    private Integer ackDeadlineSeconds = 60;

    @Parameter(
        names = "--thread_count",
        description = "The number of threads to use for delayed acks.")
    private Integer threadCount = 8;

    @Parameter(
        names = "--subscriber_count",
        description = "The number of subscriber clients to create.")
    private Integer subscriberCount = 1;

    @Parameter(
        names = "--pull_count",
        description =
            "The number of simultaneous pulls to have outstanding when using pull clients.")
    private Integer pullCount = 10;

    @Parameter(
        names = "-max_pull_messages",
        description = "The maximum number of messages to pull in a single pull request.")
    private Integer maxPullMessages = 100;

    @Parameter(
        names = "--subscriber_stream_count",
        description = "The number of streams to create per subscriber.")
    private Integer subscriberStreamCount = 1;

    @Parameter(
        names = "--message_size",
        description =
            "The number of bytes per message. Set to <= 0 to generate randomly-sized messages")
    private Integer messageSize = 100;

    @Parameter(
        names = "--message_filtered_probability",
        description =
            "Probability of a message being filtered out. Valid values are from 0.0 to 1.0"
                + " inclusive.")
    private Double messageFilteredProbability = 0.0;

    @Parameter(
        names = "--subscriber_max_outstanding_message_count",
        description =
            "The maximum number of messages to allow to be outstanding to each subscriber.")
    private Long subscriberMaxOutstandingMessageCount = 10_000L;

    @Parameter(
        names = "--subscriber_max_outstanding_bytes",
        description = "The maximum number of bytes to allow to be outstanding to each subscriber")
    private Long subscriberMaxOutstandingBytes = 1_000_000_000L;

    @Parameter(
        names = "--ordering_key_count",
        description = "The number of ordering keys on which to publish messages.")
    private int orderingKeyCount = 100;

    @Parameter(
        names = "--ordering_key_choice_strategy",
        description =
            "The strategy for choosing an ordering key on which to publish the next message.")
    private OrderedProber.KeyChoiceStrategy orderingKeyChoiceStrategy =
        OrderedProber.KeyChoiceStrategy.ROUND_ROBIN;

    @Parameter(
        names = "--delivery_history_count",
        description =
            "The number message delivery information to retain per key in the event there is an"
                + " unordered delivery and we need to print out the history.")
    private int deliveryHistoryCount = 250;

    @Parameter(
        names = "--ordered_delivery",
        description = "Whether or not to deliver messages in order.",
        arity = 1)
    private boolean orderedDelivery = true;
  }

  private static final Logger logger = Logger.getLogger(Prober.class.getName());

  public static void main(String[] args) throws IOException {
    Args parsedArgs = new Args();
    JCommander.newBuilder().addObject(parsedArgs).build().parse(args);

    Prober.Builder builder;
    if (parsedArgs.orderedDelivery) {
      builder =
          OrderedProber.newBuilder()
              .setOrderingKeyCount(parsedArgs.orderingKeyCount)
              .setKeyChoiceStrategy(parsedArgs.orderingKeyChoiceStrategy)
              .setDeliveryHistoryCount(parsedArgs.deliveryHistoryCount);
    } else {
      builder = Prober.newBuilder();
    }

    builder
        .setProject(parsedArgs.project)
        .setEndpoint(parsedArgs.endpoint)
        .setShouldCleanup(parsedArgs.shouldCleanup)
        .setNoPublish(parsedArgs.noPublish)
        .setTopicName(parsedArgs.topicName)
        .setSubscriptionName(parsedArgs.subscriptionName)
        .setSubscriptionType(parsedArgs.subscriptionType)
        .setMessageFailureProbability(parsedArgs.messageFailureProbability)
        .setPublishFrequency(parsedArgs.publishFrequency)
        .setAckDelayMilliseconds(parsedArgs.ackDelayMilliseconds)
        .setAckDeadlineSeconds(parsedArgs.ackDeadlineSeconds)
        .setThreadCount(parsedArgs.threadCount)
        .setSubscriberCount(parsedArgs.subscriberCount)
        .setPullCount(parsedArgs.pullCount)
        .setMaxPullMessages(parsedArgs.maxPullMessages)
        .setSubscriberStreamCount(parsedArgs.subscriberStreamCount)
        .setMessageSize(parsedArgs.messageSize)
        .setMessageFilteredProbability(parsedArgs.messageFilteredProbability)
        .setSubscriberMaxOutstandingMessageCount(parsedArgs.subscriberMaxOutstandingMessageCount)
        .setSubscriberMaxOutstandingBytes(parsedArgs.subscriberMaxOutstandingBytes);
    Prober prober = builder.build();
    Future<?> loadFuture = Executors.newSingleThreadExecutor().submit(prober::start);
    try {
      loadFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      logger.log(Level.WARNING, "Load generator shut down: ", e);
    }
  }
}

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

import static java.lang.Math.max;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.EvictingQueue;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages an ordered test on a single topic and a single subscription with a configurable number of
 * subscriber clients. `enable_message_ordering` is set to true on the subscription and messages are
 * sent with an ordering key. Checks for the correctness of the message order received.
 */
public class OrderedProber extends Prober {
  enum KeyChoiceStrategy {
    ROUND_ROBIN,
    RANDOM
  };

  static class Builder extends Prober.Builder {
    int orderingKeyCount;
    KeyChoiceStrategy keyChoiceStrategy;
    int deliveryHistoryCount;

    public Builder setOrderingKeyCount(int orderingKeyCount) {
      this.orderingKeyCount = orderingKeyCount;
      return this;
    }

    public Builder setKeyChoiceStrategy(KeyChoiceStrategy keyChoiceStrategy) {
      this.keyChoiceStrategy = keyChoiceStrategy;
      return this;
    }

    public Builder setDeliveryHistoryCount(int deliveryHistoryCount) {
      this.deliveryHistoryCount = deliveryHistoryCount;
      return this;
    }

    public OrderedProber build() {
      return new OrderedProber(this);
    }
  }

  // DeliveredMessage holds the sequence number and messageID received by a subscriber.
  private static class DeliveredMessage {
    DeliveredMessage(Long sequenceNum, String messageId, int subscriberIndex) {
      this.sequenceNum = sequenceNum;
      this.messageId = messageId;
      this.subscriberIndex = subscriberIndex;
    }

    @Override
    public String toString() {
      return String.format(
          "<sequence number: %d, message ID: %s, subscriber index: %d>",
          sequenceNum, messageId, subscriberIndex);
    }

    Long sequenceNum;
    String messageId;
    int subscriberIndex;
  }

  static class PublishInfo {
    long lastPublishedSequenceNum = 0L;
    long lastSuccessfullyPublishedSequenceNum = 0L;
    int publishGeneration = 1;
  }

  private static final Logger logger = Logger.getLogger(OrderedProber.class.getName());
  private static final String ORDERING_KEY_SEQUENCE_NUMBER_KEY = "ordering_key_sequence_number";

  private final int orderingKeyCount;
  private final KeyChoiceStrategy keyChoiceStrategy;
  private final int deliveryHistoryCount;

  private final Random r;
  private Integer lastOrderingKey = 0;
  private final ConcurrentHashMap<Integer, PublishInfo> perKeyPublishInfo =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, DeliveredMessage> lastReceivedMessagePerKey =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, Long> largestReceivedMessagePerKey =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, Integer> orderingKeyAffinity = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, EvictingQueue<DeliveredMessage>> lastMessagesPerKey =
      new ConcurrentHashMap<>();

  public static Builder newBuilder() {
    return new Builder();
  }

  OrderedProber(Builder builder) {
    super(builder);
    this.orderingKeyCount = builder.orderingKeyCount;
    this.keyChoiceStrategy = builder.keyChoiceStrategy;
    this.deliveryHistoryCount = builder.deliveryHistoryCount;
    this.r = new Random();
  }

  @Override
  protected Publisher.Builder updatePublisherBuilder(Publisher.Builder builder) {
    return builder.setEnableMessageOrdering(true);
  }

  @Override
  protected Subscription.Builder updateSubscriptionBuilder(Subscription.Builder builder) {
    return builder.setEnableMessageOrdering(true);
  }

  @Override
  protected ApiFuture<String> publish(
      Publisher publisher, PubsubMessage message, boolean filteredOut) {
    if (keyChoiceStrategy == KeyChoiceStrategy.ROUND_ROBIN) {
      lastOrderingKey = (lastOrderingKey + 1) % orderingKeyCount;
    } else {
      lastOrderingKey = r.nextInt(orderingKeyCount);
    }
    PublishInfo publishInfo =
        perKeyPublishInfo.compute(lastOrderingKey, (k, v) -> v == null ? new PublishInfo() : v);
    synchronized (publishInfo) {
      int publishGeneration = publishInfo.publishGeneration;
      PubsubMessage.Builder messageBuilder =
          message.toBuilder().setOrderingKey(Long.toString(lastOrderingKey));
      if (!filteredOut) {
        long nextSequenceNumber = ++publishInfo.lastPublishedSequenceNum;
        messageBuilder.putAttributes(
            ORDERING_KEY_SEQUENCE_NUMBER_KEY, Long.toString(nextSequenceNumber));
      }
      PubsubMessage messageToPublish = messageBuilder.build();
      ApiFuture<String> publishFuture = publisher.publish(messageToPublish);
      publishFuture.addListener(
          () -> {
            long sequenceNum =
                Long.parseLong(
                    messageToPublish.getAttributes().get(ORDERING_KEY_SEQUENCE_NUMBER_KEY));
            synchronized (publishInfo) {
              try {
                publishFuture.get();
                publishInfo.lastSuccessfullyPublishedSequenceNum =
                    max(publishInfo.lastSuccessfullyPublishedSequenceNum, sequenceNum);
              } catch (InterruptedException | ExecutionException e) {
                logger.log(
                    Level.WARNING,
                    String.format(
                        "Publish failed for ordering key %s with ordering key sequence number %s and"
                            + " sequence number %s. Resetting publisher and republishing messages"
                            + " starting  after last successfully published sequence number.",
                        messageToPublish.getOrderingKey(),
                        messageToPublish.getAttributes().get(ORDERING_KEY_SEQUENCE_NUMBER_KEY),
                        messageToPublish.getAttributes().get(MESSAGE_SEQUENCE_NUMBER_KEY)),
                    e);
                if (publishInfo.publishGeneration == publishGeneration) {
                  // If the generation number is the same, then this round of failures has not yet
                  // been processed, so process it now.
                  publishInfo.lastPublishedSequenceNum =
                      publishInfo.lastSuccessfullyPublishedSequenceNum;
                  ++publishInfo.publishGeneration;
                  publisher.resumePublish(messageToPublish.getOrderingKey());
                }
              }
            }
          },
          executor);
      return publishFuture;
    }
  }

  @Override
  protected boolean processMessage(PubsubMessage message, int subscriberIndex) {
    boolean ack = super.processMessage(message, subscriberIndex);

    Integer orderingKey = null;
    try {
      orderingKey = Integer.parseInt(message.getOrderingKey());
    } catch (NumberFormatException e) {
      logger.log(
          Level.WARNING,
          String.format(
              "Received message with bad ordering key %s with sequence number %s and ID %s",
              message.getOrderingKey(),
              message.getAttributes().get(MESSAGE_SEQUENCE_NUMBER_KEY),
              message.getMessageId()),
          e);
    }
    Long sequenceNum = null;
    try {
      sequenceNum = Long.parseLong(message.getAttributes().get(ORDERING_KEY_SEQUENCE_NUMBER_KEY));
    } catch (NumberFormatException e) {
      logger.log(
          Level.WARNING,
          String.format(
              "Received message with bad ordering key sequence number %s with sequence number %s and"
                  + " ID %s",
              message.getAttributes().get(ORDERING_KEY_SEQUENCE_NUMBER_KEY),
              message.getAttributes().get(MESSAGE_SEQUENCE_NUMBER_KEY),
              message.getMessageId()),
          e);
    }
    if (orderingKey == null || sequenceNum == null) {
      return ack;
    }

    // Affinity changes are expected to happen, but it is still useful to track when they happen.
    Integer previousSubscriber = orderingKeyAffinity.put(orderingKey, subscriberIndex);
    if (previousSubscriber != null && !previousSubscriber.equals(subscriberIndex)) {
      logger.info(
          String.format(
              "Affinity change for key %d from %d to %d",
              orderingKey, previousSubscriber, subscriberIndex));
    }

    Long largestSequenceNum = largestReceivedMessagePerKey.get(orderingKey);
    if (largestSequenceNum == null || sequenceNum > largestSequenceNum) {
      largestReceivedMessagePerKey.put(orderingKey, sequenceNum);
      if (largestSequenceNum != null && sequenceNum > largestSequenceNum + 1) {
        logger.log(
            Level.SEVERE,
            String.format(
                "Out-of-order largest sequence number for ordering key %d: max so far %d, got %d"
                    + " (messageID: %s)",
                orderingKey, largestSequenceNum, sequenceNum, message.getMessageId()));
      }
    }

    EvictingQueue<DeliveredMessage> lastMessages =
        lastMessagesPerKey.computeIfAbsent(
            orderingKey, (Integer k) -> EvictingQueue.create(deliveryHistoryCount));
    DeliveredMessage deliveredMessage =
        new DeliveredMessage(sequenceNum, message.getMessageId(), subscriberIndex);
    lastMessages.add(deliveredMessage);

    DeliveredMessage previousDeliveredMessage =
        lastReceivedMessagePerKey.put(orderingKey, deliveredMessage);
    if (previousDeliveredMessage != null && sequenceNum <= previousDeliveredMessage.sequenceNum) {
      if (messageIdsMatch(sequenceNum, message.getMessageId(), lastMessages)) {
        logger.info(
            String.format(
                "Redelivery of message %d for ordering key %d (messageID: %s)",
                sequenceNum, orderingKey, message.getMessageId()));
      } else {
        // If the messageID does not match the ID already seen for this sequence number, it was a
        // duplicate publish and therefore we should not reset, so put back the previous sequence
        // number.
        lastReceivedMessagePerKey.put(orderingKey, previousDeliveredMessage);
        logger.info(
            String.format(
                "Unexpected message ID %s for ordering key %d with sequence number %d. This is likely"
                    + " a message published twice; skipping message.",
                message.getMessageId(), orderingKey, sequenceNum));
      }
    } else if (previousDeliveredMessage != null
        && sequenceNum > previousDeliveredMessage.sequenceNum + 1) {
      boolean messageIdsMatch = messageIdsMatch(sequenceNum, message.getMessageId(), lastMessages);
      boolean subscribersMatch = subscriberIndex == previousDeliveredMessage.subscriberIndex;
      if (messageIdsMatch && subscribersMatch) {
        logger.log(
            Level.SEVERE,
            String.format(
                "Out-of-order delivery for ordering key %d: expected %d, got %d (messageID: %s)",
                orderingKey,
                previousDeliveredMessage.sequenceNum + 1,
                sequenceNum,
                message.getMessageId()));

        // Ensure that the list of messages appears in the log without being inter-mingled with
        // other lists that could print out at the same time.
        synchronized (logger) {
          logger.info(
              String.format(
                  "Last %d messages for ordering key %s", deliveryHistoryCount, orderingKey));
          for (DeliveredMessage dm : lastMessages) {
            logger.info(String.format("    %s", dm));
          }
        }
      } else if (!messageIdsMatch) {
        // If the messageID does not match the ID already seen for this sequence number, it was a
        // duplicate publish and should not be considered an out-of-order delivery.
        logger.info(
            String.format(
                "Unexpected message ID %s for ordering key %d with sequence number %d. This is likely"
                    + " a message published twice; skipping message.",
                message.getMessageId(), orderingKey, sequenceNum));
      } else {
        // The subscriber index does not match. This means the out-of-order message is left over
        // from delivery when the server lost track of outstanding messages and shifted affinity.
        // It is expected that this could happen and should not be considered an out-of-order
        // delivery.
        logger.info(
            String.format(
                "Affinity change on key %d resulted in unexpected message: expected %d, got %d"
                    + " (messageID: %s) ",
                orderingKey,
                previousDeliveredMessage.sequenceNum + 1,
                sequenceNum,
                message.getMessageId()));
      }
    }

    return ack;
  }

  // Checks to see if 'sequenceNum' already appears in delivered messages in 'recentDeliveries'
  // Returns true iff 'sequenceNum' is not found or the messageId associated with 'sequenceNum' is
  // 'messageId'.
  private static boolean messageIdsMatch(
      Long sequenceNumber, String messageId, EvictingQueue<DeliveredMessage> recentDeliveries) {
    for (DeliveredMessage m : recentDeliveries) {
      if (sequenceNumber.equals(m.sequenceNum)) {
        return messageId.equals(m.messageId);
      }
    }
    return true;
  }
}

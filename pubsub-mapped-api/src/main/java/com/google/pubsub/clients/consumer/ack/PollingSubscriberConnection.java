/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

package com.google.pubsub.clients.consumer.ack;

import com.google.api.core.AbstractApiService;
import com.google.api.core.ApiClock;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.Distribution;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.v1.*;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import kafka.common.KafkaException;
import org.threeten.bp.Duration;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Implementation of {@link MessageDispatcher.AckProcessor} based on Cloud Pub/Sub pull and acknowledge operations.
 */
final class PollingSubscriberConnection extends AbstractApiService implements MessageDispatcher.AckProcessor {
  static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

  private static final int MAX_PER_REQUEST_CHANGES = 1000;
  private static final int DEFAULT_MAX_MESSAGES = 1000;

  private static final Logger logger =
      Logger.getLogger(PollingSubscriberConnection.class.getName());

  private final Subscription subscription;
  private final ScheduledExecutorService pollingExecutor;
  private final SubscriberFutureStub stub;
  private final MessageDispatcher messageDispatcher;
  private final int maxDesiredPulledMessages;

  public PollingSubscriberConnection(
      Subscription subscription,
      MessageReceiver receiver,
      Duration ackExpirationPadding,
      Duration maxAckExtensionPeriod,
      Distribution ackLatencyDistribution,
      SubscriberFutureStub stub,
      FlowController flowController,
      @Nullable Long maxDesiredPulledMessages,
      ScheduledExecutorService executor,
      ScheduledExecutorService systemExecutor,
      ApiClock clock) {
    this.subscription = subscription;
    this.pollingExecutor = systemExecutor;
    this.stub = stub;
    messageDispatcher =
        new MessageDispatcher(
            receiver,
            this,
            ackExpirationPadding,
            maxAckExtensionPeriod,
            ackLatencyDistribution,
            flowController,
            executor,
            systemExecutor,
            clock);
    messageDispatcher.setMessageDeadlineSeconds(subscription.getAckDeadlineSeconds());
    this.maxDesiredPulledMessages =
        maxDesiredPulledMessages != null
            ? Ints.saturatedCast(maxDesiredPulledMessages)
            : DEFAULT_MAX_MESSAGES;
    this.startAsync();
  }

  public PullResponse pullMessages(final long timeout) throws ExecutionException, InterruptedException {

    if (!isAlive()) {
      throw new KafkaException("Is not alive");
    }

    ListenableFuture<PullResponse> pullResult =
        stub.pull(
            PullRequest.newBuilder()
                .setSubscription(subscription.getName())
                .setMaxMessages(maxDesiredPulledMessages)
                .setReturnImmediately(true)
                .build());

    PullResponse response = pullResult.get();

    messageDispatcher.processReceivedMessages(response.getReceivedMessagesList(), new Runnable() {
      @Override
      public void run() {

      }
    });

    //on non-retry-fail - stop dispatcher?
    return response;
  }

  private boolean isAlive() {
    // Read state only once. Because of threading, different calls can give different results.
    State state = state();
    return state == State.RUNNING || state == State.STARTING;
  }

  public void commit() {
    messageDispatcher.processOutstandingAckOperations();
  }

  @Override
  public void sendAckOperations(
      List<String> acksToSend, List<MessageDispatcher.PendingModifyAckDeadline> ackDeadlineExtensions) {
    // Send the modify ack deadlines in batches as not to exceed the max request
    // size.
    for (MessageDispatcher.PendingModifyAckDeadline modifyAckDeadline : ackDeadlineExtensions) {
      for (List<String> ackIdChunk :
          Lists.partition(modifyAckDeadline.ackIds, MAX_PER_REQUEST_CHANGES)) {
        stub.withDeadlineAfter(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
            .modifyAckDeadline(
                ModifyAckDeadlineRequest.newBuilder()
                    .setSubscription(subscription.getName())
                    .addAllAckIds(ackIdChunk)
                    .setAckDeadlineSeconds(modifyAckDeadline.deadlineExtensionSeconds)
                    .build());
      }
    }

    for (List<String> ackChunk : Lists.partition(acksToSend, MAX_PER_REQUEST_CHANGES)) {
      stub.withDeadlineAfter(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .acknowledge(
              AcknowledgeRequest.newBuilder()
                  .setSubscription(subscription.getName())
                  .addAllAckIds(ackChunk)
                  .build());
    }
  }

  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    notifyStopped();
  }
}

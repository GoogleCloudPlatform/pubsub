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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.clients.consumer.ack.MessageDispatcher.PendingModifyAckDeadline;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import com.google.pubsub.v1.Subscription;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.KafkaException;
import org.threeten.bp.Duration;

/**
 * Implementation of {@link MessageDispatcher.AckProcessor} based on Cloud Pub/Sub pull and acknowledge operations.
 */
final class PollingSubscriberConnection extends AbstractApiService implements MessageDispatcher.AckProcessor {

  private final Subscription subscription;
  private final SubscriberFutureStub stub;
  private final MessageDispatcher messageDispatcher;
  private final int maxDesiredPulledMessages;
  private final Integer maxPerRequestChanges;
  private final Integer extensionAckTimeoutMs;

  PollingSubscriberConnection(
      Subscription subscription,
      MessageReceiver receiver,
      Duration ackExpirationPadding,
      Duration maxAckExtensionPeriod,
      Distribution ackLatencyDistribution,
      SubscriberFutureStub stub,
      FlowController flowController,
      Long maxDesiredPulledMessages,
      ScheduledExecutorService systemExecutor,
      ApiClock clock,
      Long retryBackoffMs,
      Integer maxPerRequestChanges,
      Integer extensionAckTimeoutMs) {
    this.subscription = subscription;
    this.stub = stub;
    messageDispatcher =
        new MessageDispatcher(
            receiver,
            this,
            ackExpirationPadding,
            maxAckExtensionPeriod,
            ackLatencyDistribution,
            flowController,
            systemExecutor,
            clock,
            retryBackoffMs);

    this.maxPerRequestChanges = maxPerRequestChanges;

    messageDispatcher.setMessageDeadlineSeconds(subscription.getAckDeadlineSeconds());
    this.maxDesiredPulledMessages = Ints.saturatedCast(maxDesiredPulledMessages);
    this.extensionAckTimeoutMs = extensionAckTimeoutMs;
  }

  PullResponse pullMessages(final long timeout) throws ExecutionException, InterruptedException {
    if (!isAlive()) {
      throw new KafkaException("Subscriber is not alive");
    }

    ListenableFuture<PullResponse> pullResult =
        stub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).pull(
            PullRequest.newBuilder()
                .setSubscription(subscription.getName())
                .setMaxMessages(maxDesiredPulledMessages)
                .setReturnImmediately(true)
                .build());

    PullResponse response = pullResult.get();

    messageDispatcher.processReceivedMessages(response.getReceivedMessagesList(), () -> {});

    //on non-retry-fail - stop dispatcher?
    return response;
  }

  private boolean isAlive() {
    // Read state only once. Because of threading, different calls can give different results.
    State state = state();
    return state == State.RUNNING || state == State.STARTING;
  }

  void commit(boolean sync, Long offset) {
    messageDispatcher.acknowledgePendingMessages(sync, offset);
  }

  @Override
  public ListenableFuture<List<Empty>> sendAckOperations(
      List<String> acksToSend, List<PendingModifyAckDeadline> ackDeadlineExtensions) {
    // Send the modify ack deadlines in batches as not to exceed the max request
    // size.
    for (MessageDispatcher.PendingModifyAckDeadline modifyAckDeadline : ackDeadlineExtensions) {
      for (List<String> ackIdChunk :
          Lists.partition(modifyAckDeadline.ackIds, maxPerRequestChanges)) {
        stub.withDeadlineAfter(extensionAckTimeoutMs, TimeUnit.MILLISECONDS)
            .modifyAckDeadline(
                ModifyAckDeadlineRequest.newBuilder()
                    .setSubscription(subscription.getName())
                    .addAllAckIds(ackIdChunk)
                    .setAckDeadlineSeconds(modifyAckDeadline.deadlineExtensionSeconds)
                    .build());
      }
    }

    List<ListenableFuture<Empty>> acknowledges = new ArrayList<>();

    for (List<String> ackChunk : Lists.partition(acksToSend, maxPerRequestChanges)) {
      ListenableFuture<Empty> acknowledge = stub.withDeadlineAfter(extensionAckTimeoutMs, TimeUnit.MILLISECONDS)
          .acknowledge(
              AcknowledgeRequest.newBuilder()
                  .setSubscription(subscription.getName())
                  .addAllAckIds(ackChunk)
                  .build());
      acknowledges.add(acknowledge);
    }

    return Futures.allAsList(acknowledges);
  }

  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    messageDispatcher.stop();
    notifyStopped();
  }
}

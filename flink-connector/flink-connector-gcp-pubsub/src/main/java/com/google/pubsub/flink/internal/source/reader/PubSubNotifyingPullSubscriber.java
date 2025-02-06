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
package com.google.pubsub.flink.internal.source.reader;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayDeque;
import java.util.Deque;

public class PubSubNotifyingPullSubscriber implements NotifyingPullSubscriber {
  public static class SubscriberWakeupException extends Exception {}

  public static class SubscriberShutdownException extends Exception {}

  public interface SubscriberFactory {
    Subscriber create(MessageReceiver receiver);
  }

  private final Subscriber subscriber;

  @GuardedBy("this")
  private Optional<Throwable> permanentError = Optional.absent();

  @GuardedBy("this")
  private Optional<SettableApiFuture<Void>> notification = Optional.absent();

  @GuardedBy("this")
  private final Deque<PubsubMessage> messages = new ArrayDeque<>();

  private final AckTracker ackTracker;

  public PubSubNotifyingPullSubscriber(SubscriberFactory factory, AckTracker ackTracker) {
    this.ackTracker = ackTracker;
    this.subscriber = factory.create(this::receiveMessage);
    subscriber.addListener(
        new Listener() {
          @Override
          public void failed(State unused, Throwable throwable) {
            setPermanentError(throwable);
            completeNotification(Optional.of(throwable));
          }
        },
        MoreExecutors.directExecutor());
    this.subscriber.startAsync().awaitRunning();
  }

  @Override
  public synchronized ApiFuture<Void> notifyDataAvailable() {
    if (permanentError.isPresent()) {
      return ApiFutures.immediateFailedFuture(permanentError.get());
    }
    if (!messages.isEmpty()) {
      return ApiFutures.immediateFuture(null);
    }
    if (!notification.isPresent()) {
      notification = Optional.of(SettableApiFuture.create());
    }
    return notification.get();
  }

  @Override
  public synchronized Optional<PubsubMessage> pullMessage() throws Throwable {
    if (permanentError.isPresent()) {
      throw permanentError.get();
    }
    if (messages.isEmpty()) {
      return Optional.absent();
    }
    return Optional.of(messages.pop());
  }

  @Override
  public void interruptNotify() {
    completeNotification(Optional.of(new SubscriberWakeupException()));
  }

  @Override
  public void shutdown() {
    setPermanentError(new SubscriberShutdownException());
    completeNotification(permanentError);
    messages.clear();
    // Nack all outstanding messages, so that they are redelivered quickly.
    ackTracker.nackAll();
    subscriber.stopAsync().awaitTerminated();
  }

  private synchronized void receiveMessage(
      PubsubMessage message, AckReplyConsumer ackReplyConsumer) {
    if (permanentError.isPresent()) {
      ackReplyConsumer.nack();
      completeNotification(permanentError);
      return;
    }
    ackTracker.addPendingAck(message.getMessageId(), ackReplyConsumer);
    messages.add(message);
    completeNotification(Optional.absent());
  }

  @VisibleForTesting
  synchronized void setPermanentError(Throwable t) {
    permanentError = Optional.of(t);
  }

  private synchronized void completeNotification(Optional<Throwable> t) {
    if (notification.isPresent()) {
      if (t.isPresent()) {
        notification.get().setException(t.get());
      } else {
        notification.get().set(null);
      }
      notification = Optional.absent();
    }
  }
}

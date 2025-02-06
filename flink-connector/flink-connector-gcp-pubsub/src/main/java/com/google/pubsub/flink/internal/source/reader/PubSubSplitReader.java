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
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplit;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

public class PubSubSplitReader implements SplitReader<PubsubMessage, SubscriptionSplit> {
  private final Map<String, NotifyingPullSubscriber> subscribers;
  private final Supplier<NotifyingPullSubscriber> factory;

  public PubSubSplitReader(Supplier<NotifyingPullSubscriber> factory) {
    this.factory = factory;
    this.subscribers = new HashMap<>();
  }

  private Multimap<String, PubsubMessage> getMessages() throws Throwable {
    ImmutableListMultimap.Builder<String, PubsubMessage> messages = ImmutableListMultimap.builder();
    for (Map.Entry<String, NotifyingPullSubscriber> entry : subscribers.entrySet()) {
      for (PubsubMessage m : entry.getValue().pullMessage().asSet()) {
        messages.put(entry.getKey(), m);
      }
    }
    return messages.build();
  }

  private ApiFuture<Void> notifyDataAvailable() {
    SettableApiFuture<Void> future = SettableApiFuture.create();
    subscribers
        .values()
        .forEach(
            s -> {
              ApiFuture<Void> notification = s.notifyDataAvailable();
              ApiFutures.addCallback(
                  notification,
                  new ApiFutureCallback<Void>() {
                    @Override
                    public void onFailure(Throwable t) {
                      // Ignore exceptions caused by wakeups
                      if (t instanceof PubSubNotifyingPullSubscriber.SubscriberWakeupException) {
                        future.set(null);
                      }
                      future.setException(t);
                    }

                    @Override
                    public void onSuccess(Void result) {
                      future.set(null);
                    }
                  },
                  MoreExecutors.directExecutor());
            });
    return future;
  }

  @Override
  public RecordsBySplits<PubsubMessage> fetch() throws IOException {
    RecordsBySplits.Builder<PubsubMessage> builder = new RecordsBySplits.Builder<>();
    if (subscribers.isEmpty()) {
      return builder.build();
    }
    try {
      notifyDataAvailable().get();
      getMessages().asMap().forEach(builder::addAll);
    } catch (Throwable t) {
      throw new IOException(t);
    }
    return builder.build();
  }

  @Override
  public synchronized void handleSplitsChanges(SplitsChange<SubscriptionSplit> splitsChange) {
    if (!(splitsChange instanceof SplitsAddition)) {
      throw new IllegalArgumentException("Unexpected split event " + splitsChange);
    }
    for (SubscriptionSplit newSplit : splitsChange.splits()) {
      subscribers.computeIfAbsent(newSplit.splitId(), (splitId) -> factory.get());
    }
  }

  @Override
  public void wakeUp() {
    subscribers.forEach((split, subscriber) -> subscriber.interruptNotify());
  }

  @Override
  public void close() throws Exception {
    Exception exception = null;
    for (NotifyingPullSubscriber subscriber : subscribers.values()) {
      try {
        subscriber.shutdown();
      } catch (Exception e) {
        exception = e;
      }
    }
    if (exception != null) {
      throw exception;
    }
  }
}

/*
 * Copyright 2020 Google LLC
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

package com.google.pubsub.kafka.source;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.SubscriberInterface;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class StreamingPullSubscriber implements CloudPubSubSubscriber {

  private final SubscriberInterface underlying;

  @GuardedBy("this")
  private Optional<ApiException> error = Optional.empty();

  @GuardedBy("this")
  private final Deque<ReceivedMessage> messages = new ArrayDeque<>();

  @GuardedBy("this")
  private long nextId = 0;

  @GuardedBy("this")
  private final Map<String, AckReplyConsumer> ackConsumers = new HashMap<>();

  @GuardedBy("this")
  private Optional<SettableApiFuture<Void>> notification = Optional.empty();

  public StreamingPullSubscriber(StreamingPullSubscriberFactory factory) throws ApiException {
    underlying = factory.newSubscriber(this::addMessage);
    underlying.addListener(
        new Listener() {
          @Override
          public void failed(State state, Throwable throwable) {
            fail(toApiException(throwable));
          }
        },
        MoreExecutors.directExecutor());
    underlying.startAsync().awaitRunning();
  }

  private static ApiException toApiException(Throwable t) {
    try {
      throw t;
    } catch (ApiException e) {
      return e;
    } catch (ExecutionException e) {
      return toApiException(e.getCause());
    } catch (Throwable t2) {
      return new ApiException(t2, new StatusCode() {
        @Override
        public Code getCode() {
          return Code.INTERNAL;
        }

        @Override
        public Object getTransportCode() {
          return null;
        }
      }, false);
    }
  }

  private synchronized void fail(ApiException e) {
    error = Optional.of(e);
    if (notification.isPresent()) {
      notification.get().setException(e);
      notification = Optional.empty();
    }
  }

  private synchronized void addMessage(PubsubMessage message, AckReplyConsumer consumer) {
    String ackId = Long.toString(nextId++);
    messages.add(ReceivedMessage.newBuilder().setMessage(message).setAckId(ackId).build());
    ackConsumers.put(ackId, consumer);
    if (notification.isPresent()) {
      notification.get().set(null);
      notification = Optional.empty();
    }
  }

  private synchronized ApiFuture<Void> onData() {
    if (error.isPresent()) {
      return ApiFutures.immediateFailedFuture(error.get());
    }
    if (!messages.isEmpty()) {
      return ApiFutures.immediateFuture(null);
    }
    if (!notification.isPresent()) {
      notification = Optional.of(SettableApiFuture.create());
    }
    return notification.get();
  }

  private synchronized List<ReceivedMessage> takeMessages() {
    List<ReceivedMessage> toReturn = ImmutableList.copyOf(messages);
    messages.clear();
    return toReturn;
  }

  @Override
  public void close() {
    synchronized (this) {
      if (!error.isPresent()) {
        error = Optional.of(toApiException(new Throwable("Subscriber client shut down")));
      }
    }
    underlying.stopAsync().awaitTerminated();
  }

  @Override
  public synchronized ApiFuture<List<ReceivedMessage>> pull() {
    SettableApiFuture<List<ReceivedMessage>> toReturn = SettableApiFuture.create();
    ApiFutures.addCallback(onData(), new ApiFutureCallback<Void>() {
      @Override
      public void onFailure(Throwable t) {
        toReturn.setException(toApiException(t));
      }

      @Override
      public void onSuccess(Void result) {
        toReturn.set(takeMessages());
      }
    }, MoreExecutors.directExecutor());
    return toReturn;
  }

  @Override
  public synchronized ApiFuture<Empty> ackMessages(Collection<String> ackIds) {
    ackIds.forEach(
        id -> Optional.ofNullable(ackConsumers.remove(id)).ifPresent(AckReplyConsumer::ack));
    return ApiFutures.immediateFuture(null);
  }
}

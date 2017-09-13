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

import com.google.api.client.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PublisherGrpc.PublisherImplBase;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberImplBase;
import com.google.pubsub.v1.Subscription;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fake implementation of {@link PublisherImplBase}, that can be used to test clients of a Cloud
 * Pub/Sub Publisher.
 */
class FakeSubscriberServiceImpl extends SubscriberImplBase {
  private final AtomicBoolean subscriptionInitialized = new AtomicBoolean(false);
  private String subscription = "";
  private final AtomicInteger messageAckDeadline =
      new AtomicInteger(Subscriber.MIN_ACK_DEADLINE_SECONDS);
  private final AtomicInteger getSubscriptionCalled = new AtomicInteger();
  private final List<String> acks = new ArrayList<>();
  private final List<ModifyAckDeadline> modAckDeadlines = new ArrayList<>();
  private final BlockingQueue<PullResponse> pullResponses = new LinkedBlockingDeque<>();

  public static final class ModifyAckDeadline {
    private final String ackId;
    private final long seconds;

    ModifyAckDeadline(String ackId, long seconds) {
      Preconditions.checkNotNull(ackId);
      this.ackId = ackId;
      this.seconds = seconds;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ModifyAckDeadline)) {
        return false;
      }
      ModifyAckDeadline other = (ModifyAckDeadline) obj;
      return other.ackId.equals(this.ackId) && other.seconds == this.seconds;
    }

    @Override
    public int hashCode() {
      return ackId.hashCode();
    }

    @Override
    public String toString() {
      return "Ack ID: " + ackId + ", deadline seconds: " + seconds;
    }
  }

  void enqueuePullResponse(PullResponse response) {
    pullResponses.add(response);
  }

  @Override
  public void getSubscription(
      GetSubscriptionRequest request, StreamObserver<Subscription> responseObserver) {
    getSubscriptionCalled.incrementAndGet();
    responseObserver.onNext(
        Subscription.newBuilder()
            .setName(request.getSubscription())
            .setAckDeadlineSeconds(messageAckDeadline.get())
            .setTopic("fake-topic")
            .build());
    responseObserver.onCompleted();
  }

  /** Returns the number of times getSubscription is called. */
  @VisibleForTesting
  int getSubscriptionCalledCount() {
    return getSubscriptionCalled.get();
  }

  @Override
  public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
    try {
      responseObserver.onNext(pullResponses.take());
      responseObserver.onCompleted();
    } catch (InterruptedException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void acknowledge(
      AcknowledgeRequest request, io.grpc.stub.StreamObserver<Empty> responseObserver) {
    addReceivedAcks(request.getAckIdsList());
    responseObserver.onNext(Empty.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void modifyAckDeadline(
      ModifyAckDeadlineRequest request, StreamObserver<Empty> responseObserver) {
    for (String ackId : request.getAckIdsList()) {
      addReceivedModifyAckDeadline(new ModifyAckDeadline(ackId, request.getAckDeadlineSeconds()));
    }
    responseObserver.onNext(Empty.getDefaultInstance());
    responseObserver.onCompleted();
  }

  List<String> waitAndConsumeReceivedAcks(int expectedCount) throws InterruptedException {
    synchronized (acks) {
      while (acks.size() < expectedCount) {
        acks.wait();
      }
      List<String> receivedAcksCopy = ImmutableList.copyOf(acks.subList(0, expectedCount));
      acks.removeAll(receivedAcksCopy);
      return receivedAcksCopy;
    }
  }

  List<ModifyAckDeadline> waitAndConsumeModifyAckDeadlines(int expectedCount)
      throws InterruptedException {
    synchronized (modAckDeadlines) {
      while (modAckDeadlines.size() < expectedCount) {
        modAckDeadlines.wait();
      }
      List<ModifyAckDeadline> modAckDeadlinesCopy =
          ImmutableList.copyOf(modAckDeadlines.subList(0, expectedCount));
      modAckDeadlines.removeAll(modAckDeadlinesCopy);
      return modAckDeadlinesCopy;
    }
  }

  List<String> getAcks() {
    return acks;
  }

  List<ModifyAckDeadline> getModifyAckDeadlines() {
    return modAckDeadlines;
  }

  private void addReceivedAcks(Collection<String> newAckIds) {
    synchronized (acks) {
      acks.addAll(newAckIds);
      acks.notifyAll();
    }
  }

  private void addReceivedModifyAckDeadline(ModifyAckDeadline newAckDeadline) {
    synchronized (modAckDeadlines) {
      modAckDeadlines.add(newAckDeadline);
      modAckDeadlines.notifyAll();
    }
  }
}
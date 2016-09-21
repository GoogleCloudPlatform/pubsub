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
package com.google.pubsub.flic.cps;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.flic.common.Utils;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import com.google.pubsub.v1.Subscription;
import java.util.ArrayList;
import java.util.List;

/** Consumes from Cloud Pub/Sub in a round robin scheme. */
public class CPSRoundRobinSubscriber {

  private List<SubscriberFutureStub> pullClients;
  private List<SubscriberFutureStub> ackClients;
  private int currentPullClientIdx = 0;
  private int currentAckClientIdx = 0;

  public CPSRoundRobinSubscriber(int numClients) throws Exception {
    pullClients = new ArrayList<>(numClients);
    ackClients = new ArrayList<>(numClients);
    for (int i = 0; i < numClients; ++i) {
      // Each stub gets its own channel
      pullClients.add(SubscriberGrpc.newFutureStub(Utils.createChannel()));
      ackClients.add(SubscriberGrpc.newFutureStub(Utils.createChannel()));
    }
  }

  /** Allows each PullRequest to get distributed to different clients. */
  public ListenableFuture<PullResponse> pull(PullRequest request) {
    currentPullClientIdx = (currentPullClientIdx + 1) % pullClients.size();
    return pullClients.get(currentPullClientIdx).pull(request);
  }

  /** Allows each AcknowledgeRequest to get distributed to different clients. */
  public synchronized ListenableFuture<Empty> acknowledge(AcknowledgeRequest request) {
    currentAckClientIdx = (currentAckClientIdx + 1) % ackClients.size();
    return ackClients.get(currentAckClientIdx).acknowledge(request);
  }

  /** Create a Subscription using just the same client every time. No need to distribute. */
  public ListenableFuture<Subscription> createSubscription(Subscription request) {
    return pullClients.get(0).createSubscription(request);
  }
  
  public ListenableFuture<Subscription> getSubscription(Subscription request) {
    GetSubscriptionRequest.Builder getBuilder = GetSubscriptionRequest.newBuilder();
    getBuilder.setSubscription(request.getName());
    return pullClients.get(0).getSubscription(getBuilder.build());
  }
}

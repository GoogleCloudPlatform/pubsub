/*
 * Copyright 2021 Google LLC
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

package com.google.cloud.pubsub.spark.internal;

import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriptionName;

class PullSubscriberImpl implements PullSubscriber {

  private final SubscriberStub stub;
  private final SubscriptionName subscription;

  PullSubscriberImpl(SubscriberStub stub, SubscriptionName subscription) {
    this.stub = stub;
    this.subscription = subscription;
  }

  @Override
  public PullResponse pull() {
    return stub.pullCallable().call(PullRequest.newBuilder()
        .setSubscription(subscription.toString())
        .setMaxMessages(1000).build());
  }

  @Override
  public void close() {
    stub.close();
  }
}

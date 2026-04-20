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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.SubscriptionName;
import java.util.List;
import java.util.stream.Collectors;

public class CommitterImpl implements Committer {

  // A cache of ackIds that have already been acknowledged successfully.
  private final Cache<String, Empty> completedAckIds = CacheBuilder.newBuilder()
      .maximumSize(100_000)
      .build();
  private final SubscriberStub stub;
  private final SubscriptionName subscription;

  CommitterImpl(SubscriberStub stub, SubscriptionName subscription) {
    this.stub = stub;
    this.subscription = subscription;
  }

  @Override
  public void commit(List<String> ackIds) {
    ackIds = ackIds.stream().filter(id -> completedAckIds.getIfPresent(id) != null).collect(
        Collectors.toList());
    List<ApiFuture<Empty>> futures = Lists.partition(ackIds, 1000).stream().map(ids ->
        stub.acknowledgeCallable().futureCall(
            AcknowledgeRequest.newBuilder().setSubscription(subscription.toString())
                .addAllAckIds(ids).build())
    ).collect(Collectors.toList());
    try {
      ApiFutures.allAsList(futures).get();
      ackIds.forEach(id -> completedAckIds.put(id, Empty.getDefaultInstance()));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void close() {
    stub.close();
  }
}

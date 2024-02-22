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
package com.google.pubsub.flink.internal.source.split;

import com.google.auto.value.AutoValue;
import com.google.pubsub.flink.proto.SubscriptionSplitProto;
import com.google.pubsub.v1.ProjectSubscriptionName;
import java.util.UUID;
import org.apache.flink.api.connector.source.SourceSplit;

@AutoValue
public abstract class SubscriptionSplit implements SourceSplit {
  public abstract ProjectSubscriptionName subscriptionName();

  public abstract String uid();

  public static SubscriptionSplit create(ProjectSubscriptionName subscriptionName, String uuid) {
    return new AutoValue_SubscriptionSplit(subscriptionName, uuid);
  }

  public static SubscriptionSplit create(ProjectSubscriptionName subscriptionName) {
    return create(subscriptionName, UUID.randomUUID().toString());
  }

  public static SubscriptionSplit fromProto(SubscriptionSplitProto proto) {
    return SubscriptionSplit.create(
        ProjectSubscriptionName.parse(proto.getSubscription()), proto.getUid());
  }

  public SubscriptionSplitProto toProto() {
    return SubscriptionSplitProto.newBuilder()
        .setSubscription(subscriptionName().toString())
        .setUid(uid())
        .build();
  }

  @Override
  public String splitId() {
    return String.format("%s-%s", subscriptionName().toString(), uid());
  }
}

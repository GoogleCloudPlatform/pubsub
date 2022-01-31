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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.pubsub.v1.SubscriptionName;
import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.ContinuousInputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;

public class CpsContinuousInputPartition
    implements ContinuousInputPartition<InternalRow>, Serializable {
  private final PullSubscriberFactory subscriberFactory;
  private final SubscriptionName subscription;

  public CpsContinuousInputPartition(
      PullSubscriberFactory subscriberFactory,
      SubscriptionName subscription) {
    this.subscriberFactory = subscriberFactory;
    this.subscription = subscription;
  }

  @Override
  public InputPartitionReader<InternalRow> createContinuousReader(PartitionOffset offset) {
    checkArgument(
        offset instanceof PubsubOffset, "offset is not instance of PubsubOffset");
    return new CpsContinuousInputPartitionReader(
        subscriberFactory.newSubscriber(), subscription);
  }

  @Override
  public InputPartitionReader<InternalRow> createPartitionReader() {
    return createContinuousReader(new PubsubOffset());
  }
}

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
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;
import org.apache.spark.sql.types.StructType;

public class CpsContinuousReader implements ContinuousReader {
  private final PullSubscriberFactory subscriberFactory;
  private final Committer committer;
  private final SubscriptionName subscription;
  private final int readShards;

  private Instant lastReconfigureTime = Instant.now();

  CpsContinuousReader(
      PullSubscriberFactory subscriberFactory,
      Committer committer,
      SubscriptionName subscription,
      int readShards) {
    this.subscriberFactory = subscriberFactory;
    this.committer = committer;
    this.subscription = subscription;
    this.readShards = readShards;
  }

  @Override
  public Offset mergeOffsets(PartitionOffset[] offsets) {
    PubsubOffset toReturn = new PubsubOffset();
    for (PartitionOffset offset : offsets) {
      toReturn.ackIds.addAll(((PubsubOffset) offset).ackIds);
    }
    return toReturn;
  }

  @Override
  public Offset deserializeOffset(String json) {
    return PubsubOffset.fromJson(json);
  }

  @Override
  public Offset getStartOffset() {
    return new PubsubOffset();
  }

  @Override
  public void setStartOffset(Optional<Offset> start) {
    // CPS does not support starting locations. An offset contains messages to be acknowledged.
  }

  @Override
  public void commit(Offset end) {
    checkArgument(
        end instanceof PubsubOffset, "end offset is not assignable to PubsubOffset.");
    committer.commit(((PubsubOffset) end).ackIds);
  }

  @Override
  public void stop() {
    committer.close();
  }

  @Override
  public StructType readSchema() {
    return SparkStructs.DEFAULT_SCHEMA;
  }

  @Override
  public List<InputPartition<InternalRow>> planInputPartitions() {
    List<InputPartition<InternalRow>> list = new ArrayList<>();
    for (int i = 0; i < readShards; ++i) {
      list.add(new CpsContinuousInputPartition(subscriberFactory, subscription));
    }
    return list;
  }

  @Override
  public boolean needsReconfiguration() {
    Instant now = Instant.now();
    if (Duration.between(lastReconfigureTime, now).compareTo(Duration.of(1, ChronoUnit.MINUTES)) > 0) {
      lastReconfigureTime = now;
      return true;
    }
    return false;
  }
}

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

import com.google.common.flogger.GoogleLogger;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriptionName;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousInputPartitionReader;

public class CpsContinuousInputPartitionReader
    implements ContinuousInputPartitionReader<InternalRow> {
  private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

  private final PullSubscriber subscriber;
  private final SubscriptionName subscription;
  private final PubsubOffset offset = new PubsubOffset();
  private final Queue<ReceivedMessage> queue = new ArrayDeque<>();
  private Optional<PubsubMessage> head = Optional.empty();

  CpsContinuousInputPartitionReader(PullSubscriber subscriber, SubscriptionName subscription) {
    this.subscriber = subscriber;
    this.subscription = subscription;
  }

  @Override
  public PubsubOffset getOffset() {
    return offset;
  }

  @Override
  public boolean next() {
    try {
      while (queue.isEmpty()) {
        PullResponse response = subscriber.pull();
        queue.addAll(response.getReceivedMessagesList());
      }
      ReceivedMessage message = queue.remove();
      head = Optional.of(message.getMessage());
      offset.ackIds.add(message.getAckId());
      return true;
    } catch (Throwable t) {
      throw new IllegalStateException("Failed to retrieve messages.", t);
    }
  }

  @Override
  public InternalRow get() {
    return SparkUtils.toInternalRow(head.get(), subscription);
  }

  @Override
  public void close() {
    try {
      subscriber.close();
    } catch (Exception e) {
      log.atWarning().log("Subscriber failed to close.");
    }
  }
}

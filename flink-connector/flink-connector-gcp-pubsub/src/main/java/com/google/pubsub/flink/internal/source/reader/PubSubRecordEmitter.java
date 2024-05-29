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

import com.google.protobuf.util.Timestamps;
import com.google.pubsub.flink.PubSubDeserializationSchema;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplitState;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class PubSubRecordEmitter<T>
    implements RecordEmitter<PubsubMessage, T, SubscriptionSplitState> {
  private final PubSubDeserializationSchema<T> deserializationSchema;
  private final AckTracker ackTracker;

  public PubSubRecordEmitter(
      PubSubDeserializationSchema<T> deserializationSchema, AckTracker ackTracker) {
    this.deserializationSchema = deserializationSchema;
    this.ackTracker = ackTracker;
  }

  @Override
  public void emitRecord(
      PubsubMessage message, SourceOutput<T> sourceOutput, SubscriptionSplitState state)
      throws Exception {
    try {
      sourceOutput.collect(
          deserializationSchema.deserialize(message),
          Timestamps.toMillis(message.getPublishTime()));
      ackTracker.stagePendingAck(message.getMessageId());
    } catch (Exception e) {
      throw new IOException("Failed to deserialize PubsubMessage", e);
    }
  }
}

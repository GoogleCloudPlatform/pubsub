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

import com.google.cloud.pubsub.v1.AckReplyConsumer;

/** This class tracks the lifecycle of messages in {@link PubSubSource}. */
public interface AckTracker {
  /**
   * Track a new pending ack. Acks are pending when a message has been received but not yet
   * processed by the Flink pipeline.
   *
   * <p>If there is already a pending ack for {@code messageId}, the existing ack is replaced.
   */
  void addPendingAck(String messageId, AckReplyConsumer ackReplyConsumer);

  /**
   * Stage a pending ack for the next checkpoint snapshot. Staged acks indicate that a message has
   * been emitted to the Flink pipeline and should be included in the next checkpoint.
   */
  void stagePendingAck(String messageId);

  /**
   * Prepare all staged acks to be acknowledged to Google Cloud Pub/Sub when checkpoint {@code
   * checkpointId} completes.
   */
  void addCheckpoint(long checkpointId);

  /**
   * Acknowledge all staged acks in checkpoint {@code checkpointId} and stop tracking them in this
   * {@link AckTracker}.
   */
  void notifyCheckpointComplete(long checkpointId);

  /**
   * Negatively acknowledge (nack) and stop tracking all acks currently tracked by this {@link
   * AckTracker}. Nacked messages are eligible for redelivery by Google Cloud Pub/Sub before the
   * message's ack deadline expires.
   */
  void nackAll();
}

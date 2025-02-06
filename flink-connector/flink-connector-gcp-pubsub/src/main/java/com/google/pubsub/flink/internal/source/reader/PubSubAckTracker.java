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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class PubSubAckTracker implements AckTracker {
  @GuardedBy("this")
  private final Map<String, AckReplyConsumer> pendingAcks = new HashMap<>();

  @GuardedBy("this")
  private final List<AckReplyConsumer> stagedAcks = new ArrayList<>();

  @GuardedBy("this")
  private final SortedMap<Long, List<AckReplyConsumer>> checkpoints = new TreeMap<>();

  @Override
  public synchronized void addPendingAck(String messageId, AckReplyConsumer ackReplyConsumer) {
    pendingAcks.put(messageId, ackReplyConsumer);
  }

  @Override
  public synchronized void stagePendingAck(String messageId) {
    AckReplyConsumer ackToStage = pendingAcks.remove(messageId);
    if (ackToStage != null) {
      stagedAcks.add(ackToStage);
    }
  }

  @Override
  public synchronized void addCheckpoint(long checkpointId) {
    checkpoints.put(checkpointId, new ArrayList<>(stagedAcks));
    stagedAcks.clear();
  }

  @Override
  public synchronized void notifyCheckpointComplete(long checkpointId) {
    List<AckReplyConsumer> toAck = new ArrayList<>();
    while (!checkpoints.isEmpty() && checkpoints.firstKey() <= checkpointId) {
      toAck.addAll(checkpoints.remove(checkpoints.firstKey()));
    }
    toAck.forEach((ackReplyConsumer) -> ackReplyConsumer.ack());
  }

  @Override
  public synchronized void nackAll() {
    pendingAcks.values().forEach((ackReplyConsumer) -> ackReplyConsumer.nack());
    pendingAcks.clear();
    stagedAcks.forEach((ackReplyConsumer) -> ackReplyConsumer.nack());
    stagedAcks.clear();
    checkpoints
        .values()
        .forEach(
            (ackReplyConsumers) ->
                ackReplyConsumers.forEach((ackReplyConsumer) -> ackReplyConsumer.nack()));
    checkpoints.clear();
  }
}

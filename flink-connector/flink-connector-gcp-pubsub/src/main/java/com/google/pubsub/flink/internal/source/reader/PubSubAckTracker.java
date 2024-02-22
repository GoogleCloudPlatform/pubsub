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
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class PubSubAckTracker implements AckTracker {
  @GuardedBy("this")
  private final List<AckReplyConsumer> pendingAcks = new ArrayList<>();

  @GuardedBy("this")
  private final SortedMap<Long, List<AckReplyConsumer>> checkpoints = new TreeMap<>();

  @Override
  public synchronized void addPendingAck(AckReplyConsumer ackReplyConsumer) {
    this.pendingAcks.add(ackReplyConsumer);
  }

  @Override
  public synchronized void addCheckpoint(long checkpointId) {
    checkpoints.put(checkpointId, new ArrayList<>(pendingAcks));
    pendingAcks.clear();
  }

  @Override
  public synchronized void notifyCheckpointComplete(long checkpointId) {
    List<AckReplyConsumer> toAck = new ArrayList<>();
    while (!checkpoints.isEmpty() && checkpoints.firstKey() <= checkpointId) {
      toAck.addAll(checkpoints.remove(checkpoints.firstKey()));
    }
    toAck.forEach((ackReplyConsumer) -> ackReplyConsumer.ack());
  }
}

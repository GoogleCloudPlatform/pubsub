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
package com.google.pubsub.flink.internal.source.enumerator;

import com.google.pubsub.flink.internal.source.split.SubscriptionSplit;
import com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint;
import com.google.pubsub.v1.ProjectSubscriptionName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

/**
 * A very basic {@link SplitEnumerator} implementation that assigns a single SubscriptionSplit to
 * every registered reader.
 */
public class PubSubSplitEnumerator
    implements SplitEnumerator<SubscriptionSplit, PubSubEnumeratorCheckpoint> {
  private final ProjectSubscriptionName subscriptionName;
  private final SplitEnumeratorContext<SubscriptionSplit> context;
  private final HashMap<Integer, SubscriptionSplit> readersWithAssignments;

  public PubSubSplitEnumerator(
      ProjectSubscriptionName subscriptionName,
      SplitEnumeratorContext<SubscriptionSplit> context,
      HashMap<Integer, SubscriptionSplit> readersWithAssignments) {
    this.subscriptionName = subscriptionName;
    this.context = context;
    this.readersWithAssignments = readersWithAssignments;
  }

  @Override
  public void start() {}

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

  @Override
  public void addSplitsBack(List<SubscriptionSplit> splits, int subtaskId) {
    readersWithAssignments.remove(subtaskId);
    // It's possible that the reader has recovered already, so check if it needs a new assignment.
    checkForUnassignedReaders();
  }

  @Override
  public void addReader(int subtaskId) {
    checkForUnassignedReaders();
  }

  @Override
  public PubSubEnumeratorCheckpoint snapshotState(long checkpointId) {
    List<PubSubEnumeratorCheckpoint.Assignment> assignments = new ArrayList<>();
    readersWithAssignments.forEach(
        (task, split) -> {
          assignments.add(
              PubSubEnumeratorCheckpoint.Assignment.newBuilder()
                  .setSplit(split.toProto())
                  .setSubtask(task)
                  .build());
        });
    return PubSubEnumeratorCheckpoint.newBuilder().addAllAssignments(assignments).build();
  }

  @Override
  public void close() {}

  private void checkForUnassignedReaders() {
    // Remove all assignments for readers that are no longer registered.
    Set<Integer> registeredReaders = context.registeredReaders().keySet();
    readersWithAssignments.keySet().removeIf(task -> !registeredReaders.contains(task));

    // For all readers without an assignment, assign a new Split.
    HashMap<Integer, List<SubscriptionSplit>> newAssignments = new HashMap<>();
    for (Integer reader : registeredReaders) {
      if (!readersWithAssignments.containsKey(reader)) {
        SubscriptionSplit newSplit = SubscriptionSplit.create(subscriptionName);
        readersWithAssignments.put(reader, newSplit);
        newAssignments.put(reader, Collections.singletonList(newSplit));
      }
    }
    context.assignSplits(new SplitsAssignment<>(newAssignments));
  }
}

/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.google.pubsub.flic.common;

import com.google.pubsub.flic.common.LoadtestProto.MessageIdentifier;
import java.util.*;

/** Ensures that no message loss has occurred. */
public class MessageTracker {
  // Map from publisher id to a set of sequence numbers.
  private Map<Long, Set<Long>> sentMessages = new HashMap<>();
  // Map from publisher id to a map from sequence numbers to received count.
  private Map<Long, Map<Long, Integer>> receivedMessages = new HashMap<>();

  public MessageTracker() {}

  public synchronized void addSent(Iterable<MessageIdentifier> identifiers) {
    for (MessageIdentifier identifier : identifiers) {
      long client_id = identifier.getPublisherClientId();
      long sequence_number = identifier.getSequenceNumber();

      if (!sentMessages.containsKey(client_id)) {
        sentMessages.put(client_id, new HashSet<>());
      }
      sentMessages.get(client_id).add(sequence_number);
    }
  }

  public synchronized void addReceived(Iterable<MessageIdentifier> identifiers) {
    for (MessageIdentifier identifier : identifiers) {
      long client_id = identifier.getPublisherClientId();
      long sequence_number = identifier.getSequenceNumber();

      if (!receivedMessages.containsKey(client_id)) {
        receivedMessages.put(client_id, new HashMap<>());
      }
      Map<Long, Integer> publisher_id_recv_map = receivedMessages.get(client_id);
      if (!publisher_id_recv_map.containsKey(sequence_number)) {
        publisher_id_recv_map.put(sequence_number, 0);
      }
      publisher_id_recv_map.put(sequence_number, publisher_id_recv_map.get(sequence_number) + 1);
    }
  }

  // Get the ratio of duplicate deliveries to published messages.
  public synchronized double getDuplicateRatio() {
    long duplicates = 0;
    long size = 0;
    for (Map.Entry<Long, Map<Long, Integer>> publisher_entry : receivedMessages.entrySet()) {
      for (Map.Entry<Long, Integer> sequence_number_entry : publisher_entry.getValue().entrySet()) {
        ++size;
        if (sequence_number_entry.getValue() > 1) {
          duplicates += (sequence_number_entry.getValue() - 1);
        }
      }
    }
    return ((double) duplicates) / size;
  }

  public synchronized Iterable<MessageIdentifier> getMissing() {
    ArrayList<MessageIdentifier> missing = new ArrayList<>();
    for (Map.Entry<Long, Set<Long>> published : sentMessages.entrySet()) {
      Map<Long, Integer> received_counts =
          receivedMessages.getOrDefault(published.getKey(), new HashMap<>());
      for (Long sequence_number : published.getValue()) {
        if (received_counts.getOrDefault(sequence_number, 0) == 0) {
          missing.add(
              MessageIdentifier.newBuilder()
                  .setPublisherClientId(published.getKey())
                  .setSequenceNumber(sequence_number.intValue())
                  .build());
        }
      }
    }
    return missing;
  }
}

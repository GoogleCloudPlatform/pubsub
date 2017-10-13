// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////

package com.google.pubsub.flic.controllers;

import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import com.google.pubsub.flic.common.LoadtestProto.MessageIdentifier;

/** Ensures that no message loss has occurred. */
public class MessageTracker {

  final int numPublishers;
  final int numMessagesPerPublisher;
  Set<MessageIdentifier> duplicates = new HashSet<>();
  Map<Long, Set<MessageIdentifier>> receivedMessages = new HashMap<>();

  public MessageTracker(int numMessagesPerPublisher, int numPublishers) {
    this.numMessagesPerPublisher = numMessagesPerPublisher;
    this.numPublishers = numPublishers;
  }

  synchronized void addAllMessageIdentifiers(Iterable<MessageIdentifier> identifiers) {
    identifiers.forEach(
        identifier -> {
          receivedMessages.putIfAbsent(
              identifier.getPublisherClientId(), new HashSet<MessageIdentifier>());
          // Needs to create new object, for the test to work correctly.
          if (!receivedMessages.get(
              identifier.getPublisherClientId()).add(
                  MessageIdentifier.newBuilder()
                      .setPublisherClientId(identifier.getPublisherClientId())
                      .setSequenceNumber(identifier.getSequenceNumber())
                      .build())) {
            duplicates.add(identifier);
          }
        });
  }

  synchronized Set<MessageIdentifier> getDuplicates() {
    return duplicates;
  }

  public synchronized long getNumberReceivedMessages() {
    long counter = 0;
    for (Set<MessageIdentifier> set : receivedMessages.values()) {
      counter += set.size();
    }
    return counter;
  }

  public synchronized Iterable<MessageIdentifier> getMissing() {
    Set<MessageIdentifier> missing = new HashSet<>();
    receivedMessages.forEach(
        (id, receivedForPublisher) -> {
          for (int i = 0; i < numMessagesPerPublisher; i++) {
            MessageIdentifier message =
                MessageIdentifier.newBuilder()
                    .setPublisherClientId(id)
                    .setSequenceNumber(i)
                    .build();
            if (!receivedForPublisher.contains(message)) {
              missing.add(message);
            }
          }
        });
    for (long id = receivedMessages.keySet().size(); id < numPublishers; id++) {
      for (int i = 0; i < numMessagesPerPublisher; i++) {
        missing.add(
            MessageIdentifier.newBuilder().setPublisherClientId(id).setSequenceNumber(i).build());
      }
    }
    return missing;
  }
}
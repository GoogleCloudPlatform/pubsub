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

import com.google.pubsub.flic.common.LoadtestProto.MessageIdentifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Ensures that no message loss has occurred. */
public class MessageTracker {

  Map<Long, List<MessageIdentifier>> receivedMessages = new HashMap<>();
  final int numMessagesPerPublisher;
  final int numPublishers;

  public MessageTracker(int numMessagesPerPublisher, int numPublishers) {
    this.numMessagesPerPublisher = numMessagesPerPublisher;
    this.numPublishers = numPublishers;
  }

  synchronized void addAllMessageIdentifiers(List<MessageIdentifier> identifiers) {
    identifiers.forEach(identifier -> {
      receivedMessages.putIfAbsent(identifier.getPublisherClientId(), new ArrayList());
      receivedMessages.get(identifier.getPublisherClientId()).add(identifier);
    });
  }

  public synchronized List<MessageIdentifier> getMissing() {
    List<MessageIdentifier> missing = new ArrayList<>();
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

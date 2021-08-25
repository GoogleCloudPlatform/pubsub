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
package com.google.pubsub.kafka.source;

import com.google.api.core.ApiFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.Collection;
import java.util.List;

/**
 * An interface for clients that want to subscribe to messages from to <a
 * href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public interface CloudPubSubSubscriber extends AutoCloseable {
  ApiFuture<List<ReceivedMessage>> pull();

  ApiFuture<Empty> ackMessages(Collection<String> ackIds);

  void close();
}

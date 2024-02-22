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
package com.google.pubsub.flink.internal.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayList;
import java.util.List;

public class PubSubFlushablePublisher implements FlushablePublisher {
  Publisher publisher;
  List<ApiFuture<String>> outstandingPublishes = new ArrayList<>();

  public PubSubFlushablePublisher(Publisher publisher) {
    this.publisher = publisher;
  }

  @Override
  public void publish(PubsubMessage message) throws InterruptedException {
    outstandingPublishes.add(publisher.publish(message));
  }

  @Override
  public void flush() {
    publisher.publishAllOutstanding();
    try {
      ApiFutures.allAsList(outstandingPublishes).get();
      outstandingPublishes.clear();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

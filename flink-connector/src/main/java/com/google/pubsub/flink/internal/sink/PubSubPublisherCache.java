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

import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.HashMap;

/** */
public class PubSubPublisherCache {
  private static final HashMap<TopicName, Publisher> publishers = new HashMap<>();

  public interface PublisherFactory {
    Publisher create(TopicName topicName) throws IOException;
  }

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(PubSubPublisherCache::close));
  }

  public static synchronized Publisher getOrCreate(
      TopicName topic, PublisherFactory publisherFactory) throws IOException {
    Publisher publisher = publishers.get(topic);
    if (publisher == null) {
      publisher = publisherFactory.create(topic);
      publishers.put(topic, publisher);
    }
    return publisher;
  }

  @VisibleForTesting
  static void close() {
    publishers.forEach((topic, publisher) -> publisher.shutdown());
    publishers.clear();
  }
}

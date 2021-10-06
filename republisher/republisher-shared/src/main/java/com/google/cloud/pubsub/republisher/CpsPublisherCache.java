/*
 * Copyright 2021 Google LLC
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

// Package hack to access unsettable topicName member of builder.
package com.google.cloud.pubsub.v1;

import com.google.cloud.pubsub.republisher.PublisherCache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Duration;

public class CpsPublisherCache implements PublisherCache {
  @GuardedBy("this")
  private final Publisher.Builder builder;
  private final LoadingCache<TopicName, PublisherInterface> cache;

  public CpsPublisherCache(Publisher.Builder builder) {
    this.builder = builder;
    this.cache = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(1))
        .build(new CacheLoader<TopicName, PublisherInterface>() {
          @Override
          public PublisherInterface load(TopicName name) throws Exception {
            return create(name);
          }
        });
  }

  private synchronized PublisherInterface create(TopicName name) throws IOException {
    builder.topicName = name.toString();
    return builder.build();
  }

  @Override
  public PublisherInterface getPublisher(String topic) throws Exception {
    return cache.get(TopicName.parse(topic));
  }
}

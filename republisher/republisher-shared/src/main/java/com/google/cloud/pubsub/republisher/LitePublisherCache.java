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

package com.google.cloud.pubsub.republisher;

import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.Publisher;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.cloud.pubsub.v1.PublisherInterface;
import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.io.IOException;
import java.time.Duration;

public class LitePublisherCache implements PublisherCache {
  @GuardedBy("this")
  private final PublisherSettings.Builder settings;
  private final LoadingCache<TopicPath, PublisherInterface> cache;

  public LitePublisherCache(PublisherSettings.Builder settings) {
    this.settings = settings;
    this.cache = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(1))
        .build(new CacheLoader<TopicPath, PublisherInterface>() {
          @Override
          public PublisherInterface load(TopicPath path) throws Exception {
            Publisher publisher = create(path);
            publisher.addListener(new Listener() {
              @Override
              public void failed(State from, Throwable failure) {
                cache.invalidate(path);
              }
            }, SystemExecutors.getFuturesExecutor());
            publisher.startAsync().awaitRunning();
            return publisher;
          }
        });
  }

  private synchronized Publisher create(TopicPath path) throws IOException {
    settings.setTopicPath(path);
    return Publisher.create(settings.build());
  }

  @Override
  public PublisherInterface getPublisher(String topic) throws Exception {
    return cache.get(TopicPath.parse(topic));
  }
}

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

import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.pubsub.v1.CpsPublisherCache;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import com.google.pubsub.v1.TopicName;

public final class PublisherCaches {
  private PublisherCaches() {}

  public static PublisherCache create(Framework framework) {
    PublisherCache cpsCache = new CpsPublisherCache(Publisher.newBuilder(TopicName.newBuilder().setTopic("fake").setProject("fake").build()).setExecutorProvider(
        FixedExecutorProvider.create(SystemExecutors.getFuturesExecutor())));
    PublisherCache liteCache = new LitePublisherCache(PublisherSettings.newBuilder().setFramework(framework));
    return new RoutingPublisherCache(cpsCache, liteCache);
  }
}

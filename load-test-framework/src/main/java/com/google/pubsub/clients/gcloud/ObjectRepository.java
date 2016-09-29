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
package com.google.pubsub.clients.gcloud;

import com.google.cloud.pubsub.*;
import com.google.common.base.MoreObjects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Holds topics and subscriptions needed by the rest of the load test, creating them on demand.
 * <p>
 * For efficiency, this will only get-or-create objects once. After that, the object is assumed
 * to still exist for the life of the program.
 */

public class ObjectRepository {

  private static final Logger log = LoggerFactory.getLogger(ObjectRepository.class);
  private final LoadingCache<String, Topic> topicCache;
  private final LoadingCache<SubscriptionCacheKey, Subscription> subscriptionCache;
  private final Boolean recreateTopics;
  private final PubSub pubSub;

  @Inject
  public ObjectRepository(
      PubSub pubsub,
      @Named("recreate_topics") Boolean recreateTopics) {
    this.pubSub = pubsub;
    this.recreateTopics = recreateTopics;

    topicCache = CacheBuilder.newBuilder()
        .recordStats()
        .build(
            new CacheLoader<String, Topic>() {
              @Override
              public Topic load(String key) throws Exception {
                return getOrCreateTopic(key);
              }
            });
    subscriptionCache = CacheBuilder.newBuilder()
        .recordStats()
        .build(
            new CacheLoader<SubscriptionCacheKey, Subscription>() {
              @Override
              public Subscription load(SubscriptionCacheKey key) throws Exception {
                return getOrCreateSubscription(key);
              }
            });
  }

  /**
   * Creates a topic with the given name, if one does not yet exist.
   */
  public void createTopic(String topicName) throws ExecutionException {
    topicCache.get(topicName);
  }

  /**
   * Creates a subscription with the given parameters, if one does not yet exist.
   */
  public void createSubscription(
      String topicName, String subscriptionName) throws ExecutionException {
    SubscriptionCacheKey key =
        new SubscriptionCacheKey()
            .setName(subscriptionName)
            .setTopic(topicName);

    subscriptionCache.get(key);
  }

  private Topic getOrCreateTopic(String topicName) {
    Topic existingTopic = pubSub.getTopic(topicName);
    if (existingTopic != null) {
      if (!recreateTopics) {
        return existingTopic;
      }
      log.info("Recreating topic named: " + existingTopic.name());
      pubSub.deleteTopic(topicName);
    }
    try {
      return pubSub.create(TopicInfo.of(topicName));
    } catch (PubSubException e2) {
      log.warn("Could not create topic", e2);
      return null;
    }
  }

  private Subscription getOrCreateSubscription(SubscriptionCacheKey key) {
    try {
      Subscription subscription = pubSub.getSubscription(key.getName());
      if (subscription != null) {
        log.info("Got existing subscription: " + subscription.toString());
        final String existingTopic = MoreObjects.firstNonNull(subscription.topic().topic(), "");
        if (!existingTopic.equals(key.getTopic())) {
          log.warn("Subscription out of date; deleting subscription and recreating it.");
          pubSub.deleteSubscription(key.getName());
        }
      } else {
        try {
          log.info("(Re)creating subscription: " + key.getName());
          subscription = pubSub.create(SubscriptionInfo.of(key.getTopic(), key.getName()));
          log.info("Successfully created subscription: " + subscription);
          return subscription;
        } catch (PubSubException e2) {
          log.warn("Could not create subscription", e2);
          return null;
        }
      }
      return subscription;
    } catch (Exception e) {
      log.warn(
          "Error occurred trying to get/create a subscription: " + key.getName(), e);
      throw e;
    }
  }

  private static class SubscriptionCacheKey {
    private String name;
    private PushConfig pushConfig;
    private String topic;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SubscriptionCacheKey)) {
        return false;
      }
      SubscriptionCacheKey key = (SubscriptionCacheKey) o;
      return Objects.equals(name, key.getName());
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }

    public String getName() {
      return name;
    }

    public SubscriptionCacheKey setName(String name) {
      this.name = name;
      return this;
    }

    public SubscriptionCacheKey setPushConfig(PushConfig pushConfig) {
      this.pushConfig = pushConfig;
      return this;
    }

    public String getTopic() {
      return topic;
    }

    public SubscriptionCacheKey setTopic(String topic) {
      this.topic = topic;
      return this;
    }
  }
}

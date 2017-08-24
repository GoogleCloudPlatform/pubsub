/* Copyright 2017 Google Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License. */

package com.google.pubsub.clients.consumer;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfigCreator;
import org.apache.kafka.common.serialization.Deserializer;

class Config<K, V> {

  private final Boolean allowSubscriptionCreation;
  private final Boolean allowSubscriptionDeletion;

  private final String groupId;
  private final int maxPollRecords;

  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;

  Config(Map<String, Object> configs) {
    this(ConsumerConfigCreator.getConsumerConfig(configs),
        new PubSubConsumerConfig(configs),
        null,
        null);
  }

  Config(Map<String, Object> configs, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this(ConsumerConfigCreator.getConsumerConfig(
        ConsumerConfig.addDeserializerToConfig(
            configs,
            keyDeserializer,
            valueDeserializer)),
        new PubSubConsumerConfig(configs),
        keyDeserializer,
        valueDeserializer);
  }

  Config(Properties properties) {
    this(ConsumerConfigCreator.getConsumerConfig(properties),
        new PubSubConsumerConfig(properties),
        null,
        null);
  }

  Config(Properties properties, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this(ConsumerConfigCreator.getConsumerConfig(
        ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
        new PubSubConsumerConfig(properties),
        keyDeserializer,
        valueDeserializer);
  }

  private Config(ConsumerConfig consumerConfig,
      PubSubConsumerConfig pubSubConsumerConfig,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {

    //Kafka-specific options
    this.keyDeserializer = handleDeserializer(consumerConfig,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer, true);
    this.valueDeserializer = handleDeserializer(consumerConfig,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer, false);
    this.groupId = consumerConfig.getString(ConsumerConfig.GROUP_ID_CONFIG);
    //this is a limit on each poll for each topic
    this.maxPollRecords = consumerConfig.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);

    //PubSub-specific options
    this.allowSubscriptionCreation =
        pubSubConsumerConfig.getBoolean(PubSubConsumerConfig.SUBSCRIPTION_ALLOW_CREATE_CONFIG);
    this.allowSubscriptionDeletion =
        pubSubConsumerConfig.getBoolean(PubSubConsumerConfig.SUBSCRIPTION_ALLOW_DELETE_CONFIG);

    Preconditions.checkNotNull(this.allowSubscriptionCreation);
    Preconditions.checkNotNull(this.allowSubscriptionDeletion);
    Preconditions.checkNotNull(this.groupId);
    Preconditions.checkArgument(!this.groupId.isEmpty());
  }

  Boolean getAllowSubscriptionCreation() {
    return allowSubscriptionCreation;
  }

  Boolean getAllowSubscriptionDeletion() {
    return allowSubscriptionDeletion;
  }

  String getGroupId() {
    return groupId;
  }

  int getMaxPollRecords() {
    return maxPollRecords;
  }

  Deserializer<K> getKeyDeserializer() {
    return keyDeserializer;
  }

  Deserializer<V> getValueDeserializer() {
    return valueDeserializer;
  }

  private Deserializer handleDeserializer(ConsumerConfig configs, String configString,
      Deserializer providedDeserializer, boolean isKey) {
    Deserializer deserializer;
    if (providedDeserializer == null) {
      deserializer = configs.getConfiguredInstance(configString, Deserializer.class);
      deserializer.configure(configs.originals(), isKey);
    } else {
      configs.ignore(configString);
      deserializer = providedDeserializer;
    }
    return deserializer;
  }

}

// Copyright 2017 Google Inc.
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

package com.google.pubsub.clients.producer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerConfigAdapter;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;

/**
 * Provides the configurations for a Kafka Mapped Producer instance.
 */
public class Config<K, V> {

  private final String project;
  private final long elementCount;
  private final boolean autoCreate;

  private final int retries;
  private final int timeout;
  private final int batchSize;
  private final long lingerMs;
  private final long retriesMs;
  private final long bufferMemorySize;
  private final boolean acks;
  private final String clientId;

  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final ProducerInterceptors<K, V> interceptors;

  private static final AtomicInteger CLIENT_ID = new AtomicInteger(1);

  private final ProducerConfig kafkaConfigs;
  private final PubSubProducerConfig additionalConfigs;

  Config(Map configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    additionalConfigs = new PubSubProducerConfig(configs);

    if (keySerializer == null && valueSerializer == null) {
      kafkaConfigs = ProducerConfigAdapter.getProducerConfig(
          ProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer));
    } else {
      kafkaConfigs = ProducerConfigAdapter.getProducerConfig(configs);
    }

    // CPS's configs
    project = additionalConfigs.getString(PubSubProducerConfig.PROJECT_CONFIG);
    elementCount = additionalConfigs.getLong(PubSubProducerConfig.ELEMENTS_COUNT_CONFIG);
    autoCreate = additionalConfigs.getBoolean(PubSubProducerConfig.AUTO_CREATE_TOPICS_CONFIG);

    // Kafka's configs
    if (keySerializer == null) {
      this.keySerializer = kafkaConfigs.getConfiguredInstance(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);

      this.keySerializer.configure(kafkaConfigs.originals(), true);
    } else {
      kafkaConfigs.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
      this.keySerializer = keySerializer;
    }

    if (valueSerializer == null) {
      this.valueSerializer = kafkaConfigs.getConfiguredInstance(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);

      this.valueSerializer.configure(kafkaConfigs.originals(), false);
    } else {
      kafkaConfigs.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
      this.valueSerializer = valueSerializer;
    }

    retries = kafkaConfigs.getInt(ProducerConfig.RETRIES_CONFIG);
    timeout = kafkaConfigs.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
    retriesMs = kafkaConfigs.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
    lingerMs = kafkaConfigs.getLong(ProducerConfig.LINGER_MS_CONFIG);
    batchSize = kafkaConfigs.getInt(ProducerConfig.BATCH_SIZE_CONFIG);
    acks = kafkaConfigs.getString(ProducerConfig.ACKS_CONFIG).matches("(-)?1|all");
    bufferMemorySize = kafkaConfigs.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);

    String Id = kafkaConfigs.getString(ProducerConfig.CLIENT_ID_CONFIG);
    if (Id.length() <= 0) {
      clientId = "producer-" + CLIENT_ID.getAndIncrement();
    } else {
      clientId = Id;
    }

    kafkaConfigs.originals().put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    List<ProducerInterceptor<K, V>> interceptorList =
        (List) (ProducerConfigAdapter
            .getProducerConfig(kafkaConfigs.originals()))
            .getConfiguredInstances(
                ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptor.class);
    this.interceptors =
        interceptorList.isEmpty() ? null : new ProducerInterceptors<>(interceptorList);
  }

  String getProject() {
    return project;
  }

  boolean getAutoCreate() {
    return autoCreate;
  }

  long getElementCount() {
    return elementCount;
  }

  int getBatchSize() {
    return batchSize;
  }

  long getLingerMs() {
    return lingerMs;
  }

  long getBufferMemorySize() {
    return bufferMemorySize;
  }

  int getRetries() {
    return retries;
  }

  long getRetriesMs() {
    return retriesMs;
  }

  int getTimeout() {
    return timeout;
  }

  ProducerInterceptors<K,V> getInterceptors() {
    return interceptors;
  }

  Serializer<K> getKeySerializer() {
    return keySerializer;
  }

  Serializer<V> getValueSerializer() {
    return valueSerializer;
  }

  boolean getAcks() {
    return acks;
  }

  String getClientId() {
    return clientId;
  }
}
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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.client.util.Base64;
import com.google.api.core.ApiFutureCallback;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.SettableFuture;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.clients.producer.ExtendedConfig.PubSubConfig;

import java.io.IOException;

import org.joda.time.DateTime;
import org.threeten.bp.Duration;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerConfigAdapter;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Kafka client that publishes records to Google Cloud Pub/Sub.
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

  private static final long VERSION = 1L;
  private static final double MULTIPLIER = 1.0;
  private static final AtomicInteger CLIENT_ID = new AtomicInteger(1);

  private ExtendedConfig producerConfig;
  private Map<String, Publisher> publishers;

  private final String project;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;

  private final int retries;
  private final int timeout;
  private final int batchSize;
  private final long lingerMs;
  private final long retriesMs;
  private final long elementCount;
  private final long bufferMemorySize;
  private final boolean isAcks;
  private final boolean autoCreate;
  private final String clientId;
  private final ProducerInterceptors<K, V> interceptors;

  private final AtomicBoolean closed;

  public KafkaProducer(Map<String, Object> configs) {
    this(new ExtendedConfig(configs), null, null);
  }

  public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this(new ExtendedConfig(ProducerConfig.addSerializerToConfig(
        configs, keySerializer, valueSerializer)), keySerializer, valueSerializer);
  }

  public KafkaProducer(Properties properties) {
    this(new ExtendedConfig(properties), null, null);
  }

  public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this(new ExtendedConfig(ProducerConfig.addSerializerToConfig(
        properties, keySerializer, valueSerializer)), keySerializer, valueSerializer);
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  public KafkaProducer(ExtendedConfig configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {

    producerConfig = configs;

    // CPS's configs
    project = configs.getAdditionalConfigs().getString(PubSubConfig.PROJECT_CONFIG);
    autoCreate = configs.getAdditionalConfigs().getBoolean(PubSubConfig.AUTO_CREATE_CONFIG);
    elementCount = configs.getAdditionalConfigs().getLong(PubSubConfig.ELEMENTS_COUNT_CONFIG);

    // Kafka's configs
    if (keySerializer == null) {
      this.keySerializer = configs.getKafkaConfigs().getConfiguredInstance(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);

      this.keySerializer.configure(configs.getKafkaConfigs().originals(), true);
    } else {
      configs.getKafkaConfigs().ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
      this.keySerializer = keySerializer;
    }

    if (valueSerializer == null) {
      this.valueSerializer = configs.getKafkaConfigs().getConfiguredInstance(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);

      this.valueSerializer.configure(configs.getKafkaConfigs().originals(), false);
    } else {
      configs.getKafkaConfigs().ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
      this.valueSerializer = valueSerializer;
    }

    closed = new AtomicBoolean(false);
    retries = configs.getKafkaConfigs().getInt(ProducerConfig.RETRIES_CONFIG);
    timeout = configs.getKafkaConfigs().getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
    retriesMs = configs.getKafkaConfigs().getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
    lingerMs = configs.getKafkaConfigs().getLong(ProducerConfig.LINGER_MS_CONFIG);
    batchSize = configs.getKafkaConfigs().getInt(ProducerConfig.BATCH_SIZE_CONFIG);
    isAcks = configs.getKafkaConfigs().getString(ProducerConfig.ACKS_CONFIG).matches("(-)?1|all");
    bufferMemorySize = configs.getKafkaConfigs().getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);

    String Id = configs.getKafkaConfigs().getString(ProducerConfig.CLIENT_ID_CONFIG);
    if (Id.length() <= 0) {
      clientId = "producer-" + CLIENT_ID.getAndIncrement();
    } else {
      clientId = Id;
    }

    configs.getKafkaConfigs().originals().put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    List<ProducerInterceptor<K, V>> interceptorList =
        (List) (ProducerConfigAdapter.getConsumerConfig(configs.getKafkaConfigs().originals())).getConfiguredInstances(
            ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptor.class);
    this.interceptors =
        interceptorList.isEmpty() ? null : new ProducerInterceptors<>(interceptorList);
    
    publishers = new ConcurrentHashMap<>();
  }

  private Publisher createPublisher(String topic) {
    if (closed.get()) {
      throw new IllegalStateException("Cannot send after the producer is closed.");
    }

    Publisher pub = null;
    TopicName topicName = null;
    TopicAdminClient topicAdmin = null;

    try {
      topicName = TopicName.create(project, topic);
      topicAdmin = TopicAdminClient.create();
      topicAdmin.getTopic(topicName);
    } catch (Exception e) {
      if (e.getMessage().contains("NOT_FOUND") && autoCreate) {
        topicAdmin.createTopic(topicName);
      } else {
        throw new KafkaException("Failed to construct kafka producer, Topic not found.", e);
      }
    }

    try {
      pub = Publisher.defaultBuilder(topicName)

          .setBatchingSettings(BatchingSettings.newBuilder()
              .setElementCountThreshold(elementCount)
              .setRequestByteThreshold((long) batchSize)
              .setDelayThreshold(Duration.ofMillis(lingerMs))

              .setFlowControlSettings(FlowControlSettings.newBuilder()
                  .setMaxOutstandingRequestBytes(bufferMemorySize)
                  .build())

              .build())

          .setRetrySettings(RetrySettings.newBuilder()
              .setMaxAttempts(retries)
              .setRetryDelayMultiplier(MULTIPLIER)
              .setInitialRetryDelay(Duration.ofMillis(retriesMs))
              .setMaxRetryDelay(Duration.ofMillis((retries + 1) * retriesMs))

              .setRpcTimeoutMultiplier(MULTIPLIER)
              .setInitialRpcTimeout(Duration.ofMillis(timeout))
              .setMaxRpcTimeout(Duration.ofMillis((retries + 1) * timeout))

              .setTotalTimeout(Duration.ofMillis((retries + 2) * timeout))
              .build())

          .build();
    } catch (IOException e) {
      throw new KafkaException("Failed to construct kafka producer", e);
    }
    return pub;
  }

  /**
   * Sends the given record.
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> originalRecord) {
    return send(originalRecord, null);
  }

  //TODO: there is an onSendError() in the interceptors, while there are no errors.

  /**
   * Sends the given record and invokes the specified callback.
   * The given record must have the same topic as the producer.
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> originalRecord, Callback callback) {
    if (closed.get()) {
      throw new IllegalStateException("Cannot send after the producer is closed.");
    }

    ProducerRecord<K, V> record =
        this.interceptors == null ? originalRecord : this.interceptors.onSend(originalRecord);

    if (record == null) {
      throw new NullPointerException();
    }

    DateTime dateTime = new DateTime();

    byte[] valueBytes = null;
    if (record.value() != null) {
      try {
        valueBytes = valueSerializer.serialize(record.topic(), record.value());
      } catch (ClassCastException e) {
        throw new SerializationException("Can't convert value of class " +
            record.value().getClass().getName() + " to class " +
            producerConfig.getKafkaConfigs().getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
            " specified in value.serializer");
      }
    }

    if (valueBytes == null || valueBytes.length == 0) {
      throw new NullPointerException("Value cannot be null or an empty string.");
    }

    byte[] keyBytes = null;
    if (record.key() != null) {
      try {
        keyBytes = keySerializer.serialize(record.topic(), record.key());
      } catch (ClassCastException cce) {
        throw new SerializationException("Can't convert key of class " +
            record.key().getClass().getName() + " to class " +
            producerConfig.getKafkaConfigs().getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
            " specified in key.serializer");
      }
    }

    PubsubMessage msg = createMessage(record, dateTime.toString(), keyBytes, valueBytes);

    ApiFuture<String> messageIdFuture = publishers.computeIfAbsent(
            record.topic(), topic -> createPublisher(topic)).publish(msg);

    final Callback cb = callback;
    final String topic = record.topic();
    final int keySize = keyBytes == null ? 0 : keyBytes.length, valueSize = valueBytes.length;

    final SettableFuture<RecordMetadata> future = SettableFuture.create();

    if (callback != null) {
      if (isAcks) {
        ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback<String>() {
          private RecordMetadata recordMetadata =
              getRecordMetadata(topic, dateTime.getMillis(), keySize, valueSize);

          @Override
          public void onFailure(Throwable t) {
            callbackOnCompletion(cb, recordMetadata, new ExecutionException(t));
            future.setException(t);
          }

          @Override
          public void onSuccess(String result) {
            callbackOnCompletion(cb, recordMetadata, null);
            future.set(recordMetadata);
          }
        });
      } else {
        RecordMetadata recordMetadata =
            getRecordMetadata(topic, -1L, keySize, valueSize);
        callbackOnCompletion(cb, recordMetadata, null);
        future.set(recordMetadata);
      }
    }

    return future;
  }

  // Attribute's value's size shouldn't exceed 1024 bytes (256 for key)
  private PubsubMessage createMessage(ProducerRecord<K, V> record, String dateTime, byte[] key, byte[] value) {
    Map<String, String> attributes = new HashMap<>();

    attributes.put("id", clientId);

    attributes.put("timestamp", dateTime);

    attributes.put("version", Long.toString(VERSION));

    attributes.put("key", key == null ? "" : new String(Base64.encodeBase64(key)));

    if (attributes.get("key").getBytes().length > 1024) {
      throw new SerializationException("Key size should be at most 1024 bytes.");
    }

    return PubsubMessage.newBuilder().setData(ByteString.copyFrom(value)).putAllAttributes(attributes).build();
  }

  private void callbackOnCompletion(Callback cb, RecordMetadata m, Exception e) {
    if (interceptors != null) {
      interceptors.onAcknowledgement(m, e);
    }
    cb.onCompletion(m, e);
  }

  private RecordMetadata getRecordMetadata(String topic, long offset, int keySize, int valueSize) {
    return new RecordMetadata(new TopicPartition(topic, 0),
        offset, 0L, System.currentTimeMillis(), 0L, keySize, valueSize);
  }

  /**
   * Flushes records that have accumulated.
   */
  public void flush() {
    Map<String, Publisher> tempPublishers = publishers;
    publishers = new ConcurrentHashMap<>();

    try {
      for (Entry<String, Publisher> pub : tempPublishers.entrySet()) {
        pub.getValue().shutdown();
      }
    } catch (Exception e) {
      throw new InterruptException("Flush interrupted.", (InterruptedException) e);
    }
  }

  public List<PartitionInfo> partitionsFor(String topic) {
    Node[] dummy = {new Node(0, "", 0)};
    return Arrays.asList(new PartitionInfo(topic, 0, dummy[0], dummy, dummy));
  }

  public Map<MetricName, ? extends Metric> metrics() {
    return new HashMap<>();
  }

  /**
   * Closes the producer.
   */
  public void close() {
    close(0, null);
  }

  /**
   * Closes the producer with the given timeout.
   */
  public void close(long timeout, TimeUnit unit) {
    if (timeout < 0) {
      throw new IllegalArgumentException("The timeout cannot be negative.");
    }

    if (closed.getAndSet(true)) {
      throw new IllegalStateException("Cannot close a producer if already closed.");
    }

    Map<String, Publisher> tempPublishers = publishers;
    publishers = new ConcurrentHashMap<>();

    try {
      for (Entry<String, Publisher> pub : tempPublishers.entrySet()) {
        pub.getValue().shutdown();
      }

      if (interceptors != null) {
        interceptors.close();
      }

      keySerializer.close();
      valueSerializer.close();
    } catch (Exception e) {
      throw new KafkaException("Failed to close kafka producer", e);
    }
  }
}
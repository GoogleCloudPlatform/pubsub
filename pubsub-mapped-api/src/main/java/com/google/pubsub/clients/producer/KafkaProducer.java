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
import com.google.api.core.ApiFutureCallback;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.batching.BatchingSettings;

import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.SettableFuture;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.pubsub.v1.Publisher;

import java.io.IOException;

import java.util.Collections;
import java.util.Map.Entry;
import org.threeten.bp.Duration;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.BufferExhaustedException;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Kafka client that publishes records to Google Cloud Pub/Sub.
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

  private static final long TOTAL_DELAY = 60L;
  private static final double MULTIPLIER = 1.0;

  private ProducerConfig producerConfig;
  private Map<String, Publisher> publishers;

  private final String project;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;

  private final int retries;
  private final long retriesMs;
  private final int batchSize;
  private final long lingerMs;
  private final long elementCount;
  private final boolean isAcks;
  private final Boolean autoCreate;

  private final Semaphore lock;
  private final AtomicBoolean closed;

  public KafkaProducer(Map<String, Object> configs) {
    this(new ProducerConfig(configs), null, null);
  }

  public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this(new ProducerConfig(ProducerConfig.addSerializerToConfig(configs,
        keySerializer, valueSerializer)), keySerializer, valueSerializer);
  }

  public KafkaProducer(Properties properties) {
    this(new ProducerConfig(properties), null, null);
  }

  public KafkaProducer(Properties properties, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this(new ProducerConfig(ProducerConfig.addSerializerToConfig(properties,
        keySerializer, valueSerializer)), keySerializer, valueSerializer);
  }

  //TODO: Public for testing, not part of the interface.
  //TODO: Kafka's implementation throws a KafkaException upon failure, it doesn't apply here.

  @SuppressWarnings("unchecked")
  public KafkaProducer(ProducerConfig configs, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {

    producerConfig = configs;

    project = configs.getString(ProducerConfig.PROJECT_CONFIG);

    if (keySerializer == null) {
      this.keySerializer = configs.getConfiguredInstance(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
      this.keySerializer.configure(configs.originals(), true);
    } else {
      configs.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
      this.keySerializer = keySerializer;
    }

    if (valueSerializer == null) {
      this.valueSerializer = configs.getConfiguredInstance(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
      this.valueSerializer.configure(configs.originals(), false);
    } else {
      configs.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
      this.valueSerializer = valueSerializer;
    }

    lock = new Semaphore(1, true);
    closed = new AtomicBoolean(false);
    retries = configs.getInt(ProducerConfig.RETRIES_CONFIG);
    retriesMs = configs.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
    lingerMs = configs.getLong(ProducerConfig.LINGER_MS_CONFIG);
    batchSize = configs.getInt(ProducerConfig.BATCH_SIZE_CONFIG);
    autoCreate = configs.getBoolean(ProducerConfig.AUTO_CREATE_CONFIG);
    elementCount = configs.getLong(ProducerConfig.ELEMENTS_COUNT_CONFIG);
    isAcks = configs.getString(ProducerConfig.ACKS_CONFIG).matches("1|all");

    publishers = Collections.synchronizedMap(new HashMap<String, Publisher>());
  }

  private Publisher createPublisher(String topic) {
    Publisher pub = null;
    TopicName topicName = null;
    TopicAdminClient topicAdmin = null;

    try {
      topicName = TopicName.create(project, topic);
      topicAdmin = TopicAdminClient.create();
      topicAdmin.getTopic(topicName);
    } catch (Exception e) {
      if (autoCreate)
        topicAdmin.createTopic(topicName);
      else {
        throw new KafkaException("Failed to construct kafka producer, Topic not found.", e);
      }
    }

    try {
      pub = Publisher.defaultBuilder(topicName)
          .setBatchingSettings(BatchingSettings.newBuilder()
              .setElementCountThreshold(elementCount)
              .setRequestByteThreshold((long) batchSize)
              .setDelayThreshold(Duration.ofMillis(lingerMs))
              .build())
          .setRetrySettings(RetrySettings.newBuilder()
              .setMaxAttempts(retries)
              .setRetryDelayMultiplier(MULTIPLIER)
              .setInitialRetryDelay(Duration.ofMillis(retriesMs))
              .setMaxRetryDelay(Duration.ofMillis((retries + 1) * retriesMs))
              .setRpcTimeoutMultiplier(MULTIPLIER)
              .setInitialRpcTimeout(Duration.ofSeconds(TOTAL_DELAY))
              .setMaxRpcTimeout(Duration.ofSeconds(TOTAL_DELAY))
              .setTotalTimeout(Duration.ofSeconds(TOTAL_DELAY))
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
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  //TODO: there maybe a race condition/starvation between flush and send.
  //TODO: mimic all kafka's exceptions.

  /**
   * Sends the given record and invokes the specified callback.
   * The given record must have the same topic as the producer.
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    if (closed.get())
      throw new IllegalStateException("Cannot send after the producer is closed.");

    if (record == null)
      throw new NullPointerException();

    final Callback cb = callback;

    byte[] valueBytes = ByteString.EMPTY.toByteArray();
    if (record.value() != null) {
      try {
        valueBytes = valueSerializer.serialize(record.topic(), record.value());
      } catch (ClassCastException e) {
        throw new SerializationException("Can't convert value of class " +
            record.value().getClass().getName() + " to class " +
            producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
            " specified in value.serializer");
      }
    }

    if (valueBytes.length == 0)
      throw new NullPointerException("Value cannot be null or an empty string.");

    Map<String, String> attributes = new HashMap<>();

    byte[] keyBytes = ByteString.EMPTY.toByteArray();
    if (record.key() != null) {
      try {
        keyBytes = keySerializer.serialize(record.topic(), record.key());
      } catch (ClassCastException cce) {
        throw new SerializationException("Can't convert key of class " +
            record.key().getClass().getName() + " to class " +
            producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
            " specified in key.serializer");
      }
    }
    attributes.put("key", new String(keyBytes));

    checkRecordSize(Records.LOG_OVERHEAD + Record.recordSize(keyBytes, valueBytes));

    PubsubMessage msg = PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom(valueBytes))
        .putAllAttributes(attributes)
        .build();

    ApiFuture<String> messageIdFuture = null;

    try {
      lock.acquire();

      if (!publishers.containsKey(record.topic()))
        publishers.put(record.topic(), createPublisher(record.topic()));

      messageIdFuture = publishers.get(record.topic()).publish(msg);

      lock.release();
    } catch (InterruptedException e) {
      throw new InterruptException(e);
    }

    final RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(record.topic(), 0)
        , 0L, 0L, System.currentTimeMillis()
        , 0L, keyBytes.length, valueBytes.length);

    if (callback != null) {
      if (isAcks) {
        ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback<String>() {
          @Override
          public void onFailure(Throwable t) {
            cb.onCompletion(recordMetadata, new ExecutionException(t));
          }

          @Override
          public void onSuccess(String result) {
            cb.onCompletion(recordMetadata, null);
          }
        });
      } else {
        callback.onCompletion(recordMetadata, null);
      }
    }

    SettableFuture<RecordMetadata> future = SettableFuture.create();
    future.set(recordMetadata);
    return future;
  }

  private void checkRecordSize(int size) {
    if (size > batchSize) {
      throw new BufferExhaustedException("Message is " + size +
          " bytes which is larger than max batch size you have configured");
    }
  }

  /**
   * Flushes records that have accumulated.
   */
  public void flush() {
    try {
      lock.acquire();

      for (Entry<String, Publisher> pub : publishers.entrySet()) {
        pub.getValue().shutdown();
        publishers.put(pub.getKey(), createPublisher(pub.getKey()));
      }

      lock.release();
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
    if (timeout < 0)
      throw new IllegalArgumentException("The timeout cannot be negative.");

    if (closed.getAndSet(true))
      throw new IllegalStateException("Cannot close a producer if already closed.");

    try {
      lock.acquire();

      for (Entry<String, Publisher> pub : publishers.entrySet()) {
        pub.getValue().shutdown();
      }

      lock.release();
    } catch (Exception e) {
      throw new KafkaException("Failed to close kafka producer", e);
    }
  }
}
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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Kafka client that publishes records to Google Cloud Pub/Sub.
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

  private final AtomicBoolean closed;

  private static final long VERSION = 1L;
  private static final double MULTIPLIER = 1.0;

  private Config producerConfig;
  private Map<String, Publisher> publishers;

  public KafkaProducer(Map<String, Object> configs) {
    this(new Config(configs, null, null));
  }

  public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this(new Config(configs, keySerializer, valueSerializer));
  }

  public KafkaProducer(Properties properties) {
    this(new Config(properties, null, null));
  }

  public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this(new Config(properties, keySerializer, valueSerializer));
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  public KafkaProducer(Config configs) {
    producerConfig = configs;

    closed = new AtomicBoolean(false);

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
      topicName = TopicName.create(producerConfig.getProject(), topic);
      topicAdmin = TopicAdminClient.create();
      topicAdmin.getTopic(topicName);
    } catch (Exception e) {
      if (e.getMessage().contains("NOT_FOUND") && producerConfig.getAutoCreate()) {
        topicAdmin.createTopic(topicName);
      } else {
        throw new KafkaException("Failed to construct kafka producer, Topic not found.", e);
      }
    }

    try {
      pub = Publisher.defaultBuilder(topicName)

          .setBatchingSettings(BatchingSettings.newBuilder()
              .setElementCountThreshold(producerConfig.getElementCount())
              .setRequestByteThreshold((long) producerConfig.getBatchSize())
              .setDelayThreshold(Duration.ofMillis(producerConfig.getLingerMs()))

              .setFlowControlSettings(FlowControlSettings.newBuilder()
                  .setMaxOutstandingRequestBytes(producerConfig.getBufferMemorySize())
                  .build())

              .build())

          .setRetrySettings(RetrySettings.newBuilder()
              .setMaxAttempts(producerConfig.getRetries())
              .setRetryDelayMultiplier(MULTIPLIER)
              .setInitialRetryDelay(Duration.ofMillis(producerConfig.getRetriesMs()))
              .setMaxRetryDelay(Duration.ofMillis(
                  (producerConfig.getRetries() + 1) * producerConfig.getRetriesMs()))

              .setRpcTimeoutMultiplier(MULTIPLIER)
              .setInitialRpcTimeout(Duration.ofMillis(producerConfig.getTimeout()))
              .setMaxRpcTimeout(Duration.ofMillis(
                  (producerConfig.getRetries() + 1) * producerConfig.getTimeout()))

              .setTotalTimeout(Duration.ofMillis(
                  (producerConfig.getRetries() + 2) * producerConfig.getTimeout()))
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
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> originalRecord, Callback callback) {
    if (closed.get()) {
      throw new IllegalStateException("Cannot send after the producer is closed.");
    }

    ProducerRecord<K, V> record =
        producerConfig.getInterceptors() ==
            null ? originalRecord : producerConfig.getInterceptors().onSend(originalRecord);

    if (record == null) {
      throw new NullPointerException();
    }

    DateTime dateTime = new DateTime();

    byte[] valueBytes = null;
    if (record.value() != null) {
      try {
        valueBytes = producerConfig.getValueSerializer().serialize(record.topic(), record.value());
      } catch (ClassCastException e) {
        throw new SerializationException("Can't convert value of class " +
            record.value().getClass().getName() + " to class " +
            producerConfig.getValueSerializer().getClass().getName() +
            " specified in value.serializer");
      }
    }

    if (valueBytes == null || valueBytes.length == 0) {
      throw new NullPointerException("Value cannot be null or an empty string.");
    }

    byte[] keyBytes = null;
    if (record.key() != null) {
      try {
        keyBytes = producerConfig.getKeySerializer().serialize(record.topic(), record.key());
      } catch (ClassCastException cce) {
        throw new SerializationException("Can't convert key of class " +
            record.key().getClass().getName() + " to class " +
            producerConfig.getKeySerializer().getClass().getName() +
            " specified in key.serializer");
      }
    }

    PubsubMessage msg = createMessage(dateTime.getMillis(), keyBytes, valueBytes);

    ApiFuture<String> messageIdFuture = publishers.computeIfAbsent(
            record.topic(), topic -> createPublisher(topic)).publish(msg);

    final Callback cb = callback;
    final String topic = record.topic();
    final int keySize = keyBytes == null ? 0 : keyBytes.length, valueSize = valueBytes.length;

    final SettableFuture<RecordMetadata> future = SettableFuture.create();

    if (callback != null) {
      if (producerConfig.getAcks()) {
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
  private PubsubMessage createMessage(Long dateTime, byte[] key, byte[] value) {
    Map<String, String> attributes = new HashMap<>();

    attributes.put("id", producerConfig.getClientId());

    attributes.put("offset", Long.toString(dateTime));

    attributes.put("version", Long.toString(VERSION));

    attributes.put("key", key == null ? "" : new String(Base64.encodeBase64(key)));

    if (attributes.get("key").length() > 1024) {
      throw new SerializationException("Key size should be at most 1024 bytes.");
    }

    return PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom(value)).putAllAttributes(attributes).build();
  }

  private void callbackOnCompletion(Callback cb, RecordMetadata m, Exception e) {
    if (producerConfig.getInterceptors() != null) {
      producerConfig.getInterceptors().onAcknowledgement(m, e);
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
      throw new InterruptException("Flush interrupted.");
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

      if (producerConfig.getInterceptors() != null) {
        producerConfig.getInterceptors().close();
      }

      producerConfig.getKeySerializer().close();
      producerConfig.getValueSerializer().close();
    } catch (Exception e) {
      throw new KafkaException("Failed to close kafka producer", e);
    }
  }
}
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
 * This class, as Kafka's KafkaProducer, is thread safe, the best way to use it is by multithreading on a single producer.
 * In this implementation, due to differences in Kafka and Pub/Sub behavior, timestamp set here is used as an offset.
 *
 * This Producer is designed to work with our KafkaConsumer implementation.
 * Value is kept in Pub/Sub message body "data". Key is kept in Pub/Sub attributes map under "key".
 *
 * @param <K> Key
 * @param <V> value
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

  // Check if closed method was called previously
  private final AtomicBoolean closed;

  // Identify the version of the producer which have sent the message.
  private static final long VERSION = 1L;
  // The multiplier used with RetrySettings, to mirror that on Kafka's side.
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

  /**
   * This method creates a publisher for the provided topic, prior to that it checks if the topic
   * is there on the server, if it's not the case it creates it - if auto.create.config is set to true.
   *
   * @param topic The name of the topic which will be assigned to the publisher.
   */
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
   * Send the given record.
   *
   * @param originalRecord The ProducerRecord to be sent.
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> originalRecord) {
    return send(originalRecord, null);
  }

  //TODO: there is an onSendError() in the interceptors, while there are no errors.

  /**
   * Send the given record then invoke the provided callback.
   *
   * @param originalRecord The ProducerRecord to be sent.
   * @param callback The Callback to be invoked.
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> originalRecord, Callback callback) {
    if (closed.get()) {
      throw new IllegalStateException("Cannot send after the producer is closed.");
    }

    // Send the record to interceptors, if there is any - for pre-processing.
    ProducerRecord<K, V> record =
        producerConfig.getInterceptors() ==
            null ? originalRecord : producerConfig.getInterceptors().onSend(originalRecord);

    if (record == null) {
      throw new NullPointerException();
    }

    DateTime dateTime = new DateTime();

    // Serialize the key and value, into raw bytestrings.
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

    // issue the callback and notify the interceptors, if there is any.
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

  /**
   * Turn the provided data into a PubsubMessage.
   * NOTE: Attribute's value's size shouldn't exceed 1024 bytes (256 for key)
   *
   * @param dateTime Timestamp when the message was created for sending.
   * @param key The bytestring of key.
   * @param value the bytestring of value.
   */
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

  /**
   * Handles the callback thing.
   */
  private void callbackOnCompletion(Callback cb, RecordMetadata m, Exception e) {
    if (producerConfig.getInterceptors() != null) {
      producerConfig.getInterceptors().onAcknowledgement(m, e);
    }
    cb.onCompletion(m, e);
  }

  /**
   * Wrap provided data into a RecordMetadata object.
   */
  private RecordMetadata getRecordMetadata(String topic, long offset, int keySize, int valueSize) {
    return new RecordMetadata(new TopicPartition(topic, 0),
        offset, 0L, System.currentTimeMillis(), 0L, keySize, valueSize);
  }

  /**
   * Flushes all batches waiting to be sent, it's doing so by shutting down the publishers.
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

  /**
   * Not supported on our side - dummied.
   */
  public List<PartitionInfo> partitionsFor(String topic) {
    Node[] dummy = {new Node(0, "", 0)};
    return Arrays.asList(new PartitionInfo(topic, 0, dummy[0], dummy, dummy));
  }

  /**
   * Not supported on our side - dummied.
   */
  public Map<MetricName, ? extends Metric> metrics() {
    return new HashMap<>();
  }

  /**
   * Closes the producer - shutting down all the publishers and setting closed to true.
   */
  public void close() {
    close(0, null);
  }

  /**
   * Closes the producer - shutting down all the publishers and setting closed to true.
   * The timeout is useless in our implementation.
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
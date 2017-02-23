/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.pubsub.clients.producer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.common.PubsubUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketOptions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Kafka client that publishes records to Google Cloud Pub/Sub.
 */
public class PubsubProducer<K, V> implements Producer<K, V> {

  private static final Logger log = LoggerFactory.getLogger(PubsubProducer.class);

  private PublisherFutureStub publisher;
  private String project;
  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;
  private int batchSize;
  private boolean isAcks;
  private boolean closed = false;
  private Map<String, List<PubsubMessage>> perTopicBatches;
  private final int maxRequestSize;

  public PubsubProducer(Map<String, Object> configs) {
    this(new PubsubProducerConfig(configs), null, null);
  }

  public PubsubProducer(Map<String, Object> configs, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this(new PubsubProducerConfig(PubsubProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
        keySerializer, valueSerializer);
  }

  public PubsubProducer(Properties properties) {
    this(new PubsubProducerConfig(properties), null, null);
  }

  public PubsubProducer(Properties properties, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this(new PubsubProducerConfig(PubsubProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
        keySerializer, valueSerializer);
  }

  private PubsubProducer(PubsubProducerConfig configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    try {
      log.trace("Starting the Pubsub producer");
      publisher = PublisherGrpc.newFutureStub(PubsubUtils.createChannel());
      if (keySerializer == null) {
        this.keySerializer =
            configs.getConfiguredInstance(
                PubsubProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
        this.keySerializer.configure(configs.originals(), true);
      } else {
        configs.ignore(PubsubProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        this.keySerializer = keySerializer;
      }

      if (valueSerializer == null) {
        this.valueSerializer = configs.getConfiguredInstance(
            PubsubProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
        this.valueSerializer.configure(configs.originals(), false);
      } else {
        configs.ignore(PubsubProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        this.valueSerializer = valueSerializer;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    batchSize = configs.getInt(PubsubProducerConfig.BATCH_SIZE_CONFIG);
    isAcks = configs.getString(PubsubProducerConfig.ACKS_CONFIG).matches("1|all");
    project = configs.getString(PubsubProducerConfig.PROJECT_CONFIG);
    maxRequestSize = configs.getInt(PubsubProducerConfig.MAX_REQUEST_SIZE_CONFIG);
    perTopicBatches = Collections.synchronizedMap(new HashMap<>());
    log.debug("Producer successfully initialized.");
  }

  /**
   * Send the given record asynchronously and return a future which will eventually contain the response information.
   *
   * @param record The record to send
   * @return A future which will eventually contain the response information
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  /**
   * Send a record and invoke the given callback when the record has been acknowledged by the server
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    log.info("Received " + record.toString());
    if (closed) {
      throw new RuntimeException("Publisher is closed");
    }

    String topic = record.topic();
    Map<String, String> attributes = new HashMap<>();

    byte[] serializedKey = ByteString.EMPTY.toByteArray();
    if (record.key() != null) {
      serializedKey = this.keySerializer.serialize(topic, record.key());
      attributes.put(PubsubUtils.KEY_ATTRIBUTE, new String(serializedKey, StandardCharsets.ISO_8859_1));
    }

    if (project == null) {
      throw new RuntimeException("No project specified.");
    }

    byte[] valueBytes = ByteString.EMPTY.toByteArray();
    if (record.value() != null) {
      valueBytes = valueSerializer.serialize(topic, record.value());
    }

    checkRecordSize(Records.LOG_OVERHEAD + Record.recordSize(serializedKey, valueBytes));

    PubsubMessage message =
        PubsubMessage.newBuilder()
          .setData(ByteString.copyFrom(valueBytes))
          .putAllAttributes(attributes)
          .build();
    List<PubsubMessage> batch = perTopicBatches.get(topic);
    if (batch == null) {
      batch = new ArrayList<>(batchSize);
      perTopicBatches.put(topic, batch);
    }
    batch.add(message);
    if (batch.size() == batchSize) {
      log.trace("Sending a batch of messages.");
      PublishRequest request =
          PublishRequest.newBuilder()
            .setTopic(String.format(PubsubUtils.CPS_TOPIC_FORMAT, project, topic))
            .addAllMessages(batch)
            .build();
      doSend(request, callback);
    }
    return null; //new FutureRecordMetadata();
  }

  private Future<RecordMetadata> doSend(PublishRequest request, Callback callback) {
    try {
      ListenableFuture<PublishResponse> response = publisher.publish(request);
      if (callback != null) {
        if (isAcks) {
          Futures.addCallback(
              response,
              new FutureCallback<PublishResponse>() {
                public void onSuccess(PublishResponse response) {
                  perTopicBatches.clear();
                  callback.onCompletion(null, null);
                }

                public void onFailure(Throwable t) {
                  callback.onCompletion(null, new Exception(t));
                }
              }
          );
        } else {
          perTopicBatches.clear();
          callback.onCompletion(null, null);
        }
      } else {
        response.get();
        perTopicBatches.clear();
      }
    } catch (InterruptedException | ExecutionException e) {
      return new FutureFailure(e);
    }
    return null; //new FutureRecordMetadata();
  }

  private void checkRecordSize(int size) {
    if (size > this.maxRequestSize) {
      throw new RecordTooLargeException("Messge is " + size + " bytes which is larger than max request size you have"
          + " configured");
    }
  }

  /**
   * Flush any accumulated records from the producer. Blocks until all sends are complete.
   */
  public void flush() {
    throw new NotImplementedException("Not yet implemented");
  }

  /**
   * Get a list of partitions for the given topic for custom partition assignment. The partition metadata will change
   * over time so this list should not be cached.
   */
  public List<PartitionInfo> partitionsFor(String topic) {
    throw new NotImplementedException("Partitions not supported");
  }

  /**
   * Return a map of metrics maintained by the producer
   */
  public Map<MetricName, ? extends Metric> metrics() {
    throw new NotImplementedException("Metrics not supported.");
  }

  /**
   * Close this producer
   */
  public void close() {
    close(0, null);
  }

  /**
   * Tries to close the producer cleanly within the specified timeout. If the close does not complete within the
   * timeout, fail any pending send requests and force close the producer.
   */
  public void close(long timeout, TimeUnit unit) {
    if (timeout < 0) {
      throw new IllegalArgumentException("Timout cannot be negative.");
    }

    log.debug("Closed producer");
    closed = true;
  }

  /** Taken from KafkaProducer.java since FutureFailure is private inside that class. */
  private static class FutureFailure implements Future<RecordMetadata> {
    private final ExecutionException exception;

    public FutureFailure(Exception e) {
      this.exception = new ExecutionException(e);
    }

    public boolean cancel(boolean interrupt) {
      return false;
    }

    public RecordMetadata get() throws ExecutionException {
      throw this.exception;
    }

    public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
      throw this.exception;
    }

    public boolean isCancelled() {
      return false;
    }

    public boolean isDone() {
      return true;
    }
  }
}

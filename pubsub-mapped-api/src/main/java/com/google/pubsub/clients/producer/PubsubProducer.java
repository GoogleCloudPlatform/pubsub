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

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.common.PubsubChannelUtil;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A Kafka client that publishes records to Google Cloud Pub/Sub.
 */
public class PubsubProducer<K, V> implements Producer<K, V> {

  private static final Logger log = LoggerFactory.getLogger(PubsubProducer.class);

  private final PublisherFutureStub publisher;
  private final String project;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final int batchSize;
  private final boolean isAcks;
  private final Map<String, List<PubsubMessage>> perTopicBatches;
  private final int maxRequestSize;
  private final PubsubChannelUtil channelUtil;
  private final SettableFuture<RecordMetadata> future;

  private boolean closed = false;

  private PubsubProducer(Builder builder) {
    publisher = builder.publisher;
    project = builder.project;
    batchSize = builder.batchSize;
    isAcks = builder.isAcks;
    perTopicBatches = builder.perTopicBatches;
    maxRequestSize = builder.maxRequestSize;
    channelUtil = builder.channelUtil;
    keySerializer = builder.keySerializer;
    valueSerializer = builder.valueSerializer;
    future = SettableFuture.create();
  }

  public PubsubProducer(Map<String, Object> configs) {
    this(new PubsubProducerConfig(configs), null, null);
  }

  public PubsubProducer(Map<String, Object> configs, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this(new PubsubProducerConfig(
            PubsubProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
        keySerializer, valueSerializer);
  }

  public PubsubProducer(Properties properties) {
    this(new PubsubProducerConfig(properties), null, null);
  }

  public PubsubProducer(Properties properties, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this(new PubsubProducerConfig(
            PubsubProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
        keySerializer, valueSerializer);
  }

  private PubsubProducer(PubsubProducerConfig configs, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    try {
      log.trace("Starting the Pubsub producer");
      channelUtil = new PubsubChannelUtil();
      publisher = PublisherGrpc.newFutureStub(channelUtil.channel())
          .withCallCredentials(channelUtil.callCredentials());

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
    future = SettableFuture.create();
    batchSize = configs.getInt(PubsubProducerConfig.BATCH_SIZE_CONFIG);
    isAcks = configs.getString(PubsubProducerConfig.ACKS_CONFIG).matches("1|all");
    project = configs.getString(PubsubProducerConfig.PROJECT_CONFIG);
    maxRequestSize = configs.getInt(PubsubProducerConfig.MAX_REQUEST_SIZE_CONFIG);
    perTopicBatches = new HashMap<>();

    log.debug("Producer successfully initialized.");
  }

  /**
   * Sends the given record.
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  /**
   * Sends the given record and invokes the specified callback.
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    log.trace("Received " + record.toString());
    if (closed) {
      throw new RuntimeException("Publisher is closed");
    }

    String topic = record.topic();
    Map<String, String> attributes = new HashMap<>();

    byte[] serializedKey = ByteString.EMPTY.toByteArray();
    if (record.key() != null) {
      serializedKey = this.keySerializer.serialize(topic, record.key());
      attributes
          .put(PubsubChannelUtil.KEY_ATTRIBUTE, new String(serializedKey, StandardCharsets.ISO_8859_1));
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
              .setTopic(String.format(PubsubChannelUtil.CPS_TOPIC_FORMAT, project, topic))
              .addAllMessages(batch)
              .build();
      doSend(request, callback);
    }
    return future;
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
      future.setException(e);
      // TODO: fix this so it's not unused
    }
    return future;
  }

  private void checkRecordSize(int size) {
    if (size > this.maxRequestSize) {
      throw new RecordTooLargeException(
          "Message is " + size + " bytes which is larger than max request size you have"
              + " configured");
    }
  }

  /**
   * Flushes records that have accumulated.
   */
  public void flush() {
    log.debug("Flushing...");
    for (String topic : perTopicBatches.keySet()) {
      PublishRequest request =
          PublishRequest.newBuilder()
              .setTopic(String.format(channelUtil.CPS_TOPIC_FORMAT, project, topic))
              .addAllMessages(perTopicBatches.get(topic))
              .build();
      doSend(request, null);
    }
  }

  /**
   * Not supported by this implementation.
   */
  public List<PartitionInfo> partitionsFor(String topic) {
    throw new NotImplementedException("Partitions not supported");
  }

  /**
   * Not supported by this implementation.
   */
  public Map<MetricName, ? extends Metric> metrics() {
    throw new NotImplementedException("Metrics not supported.");
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
      throw new IllegalArgumentException("Timeout cannot be negative.");
    }

    channelUtil.closeChannel();
    log.debug("Closed producer");
    closed = true;
  }

  /**
   * PubsubProducer.Builder is used to create an instance of the publisher, with the specified
   * properties and configurations.
   */
  public static class Builder<K, V> {

    private final String project;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    private PubsubChannelUtil channelUtil;
    private PublisherFutureStub publisher;
    private int batchSize;
    private boolean isAcks;
    private Map<String, List<PubsubMessage>> perTopicBatches;
    private int maxRequestSize;

    public Builder(String project, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
      Preconditions
          .checkArgument(project != null && keySerializer != null && valueSerializer != null);
      this.project = project;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
      setDefaults();
    }

    private void setDefaults() {
      // this is where to set 'regular' fields w/o side effects
      this.batchSize = PubsubProducerConfig.DEFAULT_BATCH_SIZE;
      this.isAcks = PubsubProducerConfig.DEFAULT_ACKS;
      this.perTopicBatches = Collections.synchronizedMap(new HashMap<>());
      this.maxRequestSize = PubsubProducerConfig.DEFAULT_MAX_REQUEST_SIZE;
    }

    public Builder publisherFutureStub(PublisherFutureStub val) {
      publisher = val;
      return this;
    }

    public Builder batchSize(int val) {
      Preconditions.checkArgument(val > 0);
      batchSize = val;
      return this;
    }

    public Builder isAcks(boolean val) {
      isAcks = val;
      return this;
    }

    public Builder perTopicBatches(Map<String, List<PubsubMessage>> val) {
      perTopicBatches = val;
      return this;
    }

    public Builder maxRequestSize(int val) {
      Preconditions.checkArgument(val >= 0);
      maxRequestSize = val;
      return this;
    }

    public Builder pubsubChannelUtil(PubsubChannelUtil val) {
      channelUtil = val;
      return this;
    }

    public PubsubProducer build() {
      // this is where to set fields w/ side effects
      if (channelUtil == null) {
        this.channelUtil = new PubsubChannelUtil();
      }
      if (publisher == null) {
        this.publisher = PublisherGrpc.newFutureStub(channelUtil.channel())
            .withCallCredentials(channelUtil.callCredentials());
      }
      return new PubsubProducer(this);
    }
  }

}
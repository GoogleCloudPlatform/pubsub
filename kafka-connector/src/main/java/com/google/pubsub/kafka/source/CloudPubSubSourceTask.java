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
package com.google.pubsub.kafka.source;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.common.ConnectorCredentialsProvider;
import com.google.pubsub.kafka.source.CloudPubSubSourceConnector.PartitionScheme;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SourceTask} used by a {@link CloudPubSubSourceConnector} to write messages to <a
 * href="http://kafka.apache.org/">Apache Kafka</a>. Due to at-last-once semantics in Google
 * Cloud Pub/Sub duplicates in Kafka are possible.
 */
public class CloudPubSubSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSourceTask.class);
  private static final int NUM_CPS_SUBSCRIBERS = 10;

  private String kafkaTopic;
  private ProjectSubscriptionName cpsSubscription;
  private String kafkaMessageKeyAttribute;
  private String kafkaMessageTimestampAttribute;
  private boolean makeOrderingKeyAttribute;
  private int kafkaPartitions;
  private PartitionScheme kafkaPartitionScheme;
  // Keeps track of the current partition to publish to if the partition scheme is round robin.
  private int currentRoundRobinPartition = -1;
  // Keep track of all ack ids that have not been sent correctly acked yet.
  private final Set<String> deliveredAckIds = Collections.synchronizedSet(new HashSet<String>());
  private final Set<String> ackIds = Collections.synchronizedSet(new HashSet<String>());
  private CloudPubSubSubscriber subscriber;
  private final Set<String> ackIdsInFlight = Collections.synchronizedSet(new HashSet<String>());
  private final Set<String> standardAttributes = new HashSet<>();
  private boolean useKafkaHeaders;
  private final Executor ackExecutor = Executors.newCachedThreadPool();

  public CloudPubSubSourceTask() {}

  @VisibleForTesting
  public CloudPubSubSourceTask(CloudPubSubSubscriber subscriber) {
    this.subscriber = subscriber;
  }

  @Override
  public String version() {
    return new CloudPubSubSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    Map<String, Object> validatedProps = new CloudPubSubSourceConnector().config().parse(props);
    cpsSubscription = ProjectSubscriptionName.newBuilder()
        .setProject(validatedProps.get(ConnectorUtils.CPS_PROJECT_CONFIG).toString())
        .setSubscription(
            validatedProps.get(CloudPubSubSourceConnector.CPS_SUBSCRIPTION_CONFIG).toString())
        .build();
    String cpsEndpoint = (String) validatedProps.get(ConnectorUtils.CPS_ENDPOINT);
    kafkaTopic = validatedProps.get(CloudPubSubSourceConnector.KAFKA_TOPIC_CONFIG).toString();
    int cpsMaxBatchSize = (Integer) validatedProps
        .get(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG);
    kafkaPartitions =
        (Integer) validatedProps.get(CloudPubSubSourceConnector.KAFKA_PARTITIONS_CONFIG);
    kafkaMessageKeyAttribute =
        (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG);
    kafkaMessageTimestampAttribute =
        (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_MESSAGE_TIMESTAMP_CONFIG);
    kafkaPartitionScheme =
        PartitionScheme.getEnum(
            (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG));
    useKafkaHeaders = (Boolean) validatedProps.get(CloudPubSubSourceConnector.USE_KAFKA_HEADERS);
    makeOrderingKeyAttribute =
        (Boolean) validatedProps.get(CloudPubSubSourceConnector.CPS_MAKE_ORDERING_KEY_ATTRIBUTE);
    ConnectorCredentialsProvider gcpCredentialsProvider = new ConnectorCredentialsProvider();
    String gcpCredentialsFilePath = (String) validatedProps
        .get(ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG);
    String credentialsJson = (String) validatedProps
        .get(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG);
    boolean useStreamingPull = (Boolean) validatedProps
        .get(CloudPubSubSourceConnector.CPS_STREAMING_PULL_ENABLED);
    long streamingPullBytes = (Long) validatedProps
        .get(CloudPubSubSourceConnector.CPS_STREAMING_PULL_FLOW_CONTROL_BYTES);
    long streamingPullMessages = (Long) validatedProps
        .get(CloudPubSubSourceConnector.CPS_STREAMING_PULL_FLOW_CONTROL_MESSAGES);
    int streamingPullParallelPullCount = (Integer) validatedProps.get(
        CloudPubSubSourceConnector.CPS_STREAMING_PULL_PARALLEL_PULL_COUNT);
    if (gcpCredentialsFilePath != null) {
      try {
        gcpCredentialsProvider.loadFromFile(gcpCredentialsFilePath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else if (credentialsJson != null) {
      try {
        gcpCredentialsProvider.loadJson(credentialsJson);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    // Only do this if we did not set through the constructor.
    if (subscriber == null) {
      if (useStreamingPull) {
        subscriber = new StreamingPullSubscriber(
            receiver -> Subscriber.newBuilder(cpsSubscription, receiver)
                .setCredentialsProvider(gcpCredentialsProvider)
                .setFlowControlSettings(
                    FlowControlSettings.newBuilder()
                        .setLimitExceededBehavior(LimitExceededBehavior.Block)
                        .setMaxOutstandingElementCount(streamingPullMessages)
                        .setMaxOutstandingRequestBytes(streamingPullBytes).build())
                .setEndpoint(cpsEndpoint)
                .setParallelPullCount(streamingPullParallelPullCount)
                .build());
      } else {
        subscriber = new CloudPubSubRoundRobinSubscriber(NUM_CPS_SUBSCRIBERS,
            gcpCredentialsProvider,
            cpsEndpoint, cpsSubscription, cpsMaxBatchSize);
      }
    }
    standardAttributes.add(kafkaMessageKeyAttribute);
    standardAttributes.add(kafkaMessageTimestampAttribute);
    log.info("Started a CloudPubSubSourceTask.");
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    ackMessages();
    log.debug("Polling...");
    try {
      List<ReceivedMessage> response = subscriber.pull().get();
      List<SourceRecord> sourceRecords = new ArrayList<>();
      log.trace("Received " + response.size() + " messages");
      for (ReceivedMessage rm : response) {
        PubsubMessage message = rm.getMessage();
        String ackId = rm.getAckId();
        // If we are receiving this message a second (or more) times because the ack for it failed
        // then do not create a SourceRecord for this message. In case we are waiting for ack
        // response we also skip the message
        if (ackIds.contains(ackId) || deliveredAckIds.contains(ackId) || ackIdsInFlight
            .contains(ackId)) {
          continue;
        }
        ackIds.add(ackId);
        Map<String, String> messageAttributes = message.getAttributesMap();
        String key;
        String orderingKey = message.getOrderingKey();
        if (ConnectorUtils.CPS_ORDERING_KEY_ATTRIBUTE.equals(kafkaMessageKeyAttribute)) {
          key = orderingKey;
        } else {
          key = messageAttributes.get(kafkaMessageKeyAttribute);
        }
        Long timestamp = getLongValue(messageAttributes.get(kafkaMessageTimestampAttribute));
        if (timestamp == null){
          timestamp = Timestamps.toMillis(message.getPublishTime());
        }
        ByteString messageData = message.getData();
        byte[] messageBytes = messageData.toByteArray();

        boolean hasCustomAttributes = !standardAttributes.containsAll(messageAttributes.keySet())
            || (makeOrderingKeyAttribute && orderingKey != null && !orderingKey.isEmpty());

        Map<String, String> ack = Collections.singletonMap(cpsSubscription.toString(), ackId);
        SourceRecord record = null;
        if (hasCustomAttributes) {
          if (useKafkaHeaders) {
            record =
                createRecordWithHeaders(
                    messageAttributes, ack, key, orderingKey, messageBytes, timestamp);
          } else {
            record =
                createRecordWithStruct(
                    messageAttributes, ack, key, orderingKey, messageBytes, timestamp);
          }
        } else {
          record =
            new SourceRecord(
                null,
                ack,
                kafkaTopic,
                selectPartition(key, messageBytes, orderingKey),
                Schema.OPTIONAL_STRING_SCHEMA,
                key,
                Schema.BYTES_SCHEMA,
                messageBytes,
                timestamp);
        }
        sourceRecords.add(record);
      }
      return sourceRecords;
    } catch (Exception e) {
      log.info("Error while retrieving records, treating as an empty poll. " + e);
      return new ArrayList<>();
    }
  }

  private SourceRecord createRecordWithHeaders(
      Map<String, String> messageAttributes,
      Map<String, String> ack,
      String key,
      String orderingKey,
      byte[] messageBytes,
      Long timestamp) {
    ConnectHeaders headers = new ConnectHeaders();
    for (Entry<String, String> attribute :
            messageAttributes.entrySet()) {
      if (!attribute.getKey().equals(kafkaMessageKeyAttribute)) {
        headers.addString(attribute.getKey(), attribute.getValue());
      }
    }
    if (makeOrderingKeyAttribute && orderingKey != null && !orderingKey.isEmpty()) {
      headers.addString(ConnectorUtils.CPS_ORDERING_KEY_ATTRIBUTE, orderingKey);
    }

    return new SourceRecord(
            null,
            ack,
            kafkaTopic,
            selectPartition(key, messageBytes, orderingKey),
            Schema.OPTIONAL_STRING_SCHEMA,
            key,
            Schema.BYTES_SCHEMA,
            messageBytes,
            timestamp,
            headers);
  }

  private SourceRecord createRecordWithStruct(
      Map<String, String> messageAttributes,
      Map<String, String> ack,
      String key,
      String orderingKey,
      byte[] messageBytes,
      Long timestamp) {
    SchemaBuilder valueSchemaBuilder =
        SchemaBuilder.struct()
            .field(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD, Schema.BYTES_SCHEMA);

    for (Entry<String, String> attribute :
        messageAttributes.entrySet()) {
      if (!attribute.getKey().equals(kafkaMessageKeyAttribute)) {
        valueSchemaBuilder.field(attribute.getKey(),
            Schema.STRING_SCHEMA);
      }
    }
    if (makeOrderingKeyAttribute && orderingKey != null && !orderingKey.isEmpty()) {
      valueSchemaBuilder.field(ConnectorUtils.CPS_ORDERING_KEY_ATTRIBUTE, Schema.STRING_SCHEMA);
    }

    Schema valueSchema = valueSchemaBuilder.build();
    Struct value =
        new Struct(valueSchema)
            .put(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD,
                messageBytes);
    for (Field field : valueSchema.fields()) {
      if (field.name().equals(ConnectorUtils.CPS_ORDERING_KEY_ATTRIBUTE)) {
        value.put(field.name(), orderingKey);
      } else if (!field.name().equals(
          ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD)) {
        value.put(field.name(), messageAttributes.get(field.name()));
      }
    }
    return new SourceRecord(
            null,
            ack,
            kafkaTopic,
            selectPartition(key, value, orderingKey),
            Schema.OPTIONAL_STRING_SCHEMA,
            key,
            valueSchema,
            value,
            timestamp);
  }

  @Override
  public void commit() throws InterruptedException {
    ackMessages();
  }

  /**
   * Attempt to ack all ids in {@link #deliveredAckIds}.
   */
  private void ackMessages() {
    if (deliveredAckIds.size() != 0) {
      final Set<String> ackIdsBatch = new HashSet<>();
      synchronized (deliveredAckIds) {
        ackIdsInFlight.addAll(deliveredAckIds);
        ackIdsBatch.addAll(deliveredAckIds);
        deliveredAckIds.clear();
      }
      final ApiFuture<Empty> response = subscriber.ackMessages(ackIdsBatch);
      response.addListener(() -> {
        try {
          response.get();
          log.trace("Successfully acked a set of messages. {}", ackIdsBatch.size());
        } catch (Exception e) {
          deliveredAckIds.addAll(ackIdsBatch);
          log.error("An exception occurred acking messages: " + e);
        } finally {
          ackIdsInFlight.removeAll(ackIdsBatch);
        }
      }, ackExecutor);
    }
  }

  /** Return the partition a message should go to based on {@link #kafkaPartitionScheme}. */
  private Integer selectPartition(Object key, Object value, String orderingKey) {
    if (kafkaPartitionScheme.equals(PartitionScheme.HASH_KEY)) {
      return key == null ? 0 : Math.abs(key.hashCode()) % kafkaPartitions;
    } else if (kafkaPartitionScheme.equals(PartitionScheme.HASH_VALUE)) {
      return Math.abs(value.hashCode()) % kafkaPartitions;
    } else if (kafkaPartitionScheme.equals(PartitionScheme.KAFKA_PARTITIONER)) {
      return null;
    } else if (kafkaPartitionScheme.equals(PartitionScheme.ORDERING_KEY) && orderingKey != null &&
          !orderingKey.isEmpty()) {
      return Math.abs(orderingKey.hashCode()) % kafkaPartitions;
    } else {
      currentRoundRobinPartition = ++currentRoundRobinPartition % kafkaPartitions;
      return currentRoundRobinPartition;
    }
  }

  private Long getLongValue(String timestamp) {
    if (timestamp == null) {
      return null;
    }
    try {
      return Long.valueOf(timestamp);
    } catch (NumberFormatException e) {
      log.error("Error while converting `{}` to number", timestamp, e);
    }
    return null;
  }

  @Override
  public void stop() {
    if (subscriber != null) {
      subscriber.close();
    }
  }

  @Override
  public void commitRecord(SourceRecord record) {
    String ackId = record.sourceOffset().get(cpsSubscription.toString()).toString();
    deliveredAckIds.add(ackId);
    ackIds.remove(ackId);
    log.trace("Committed {}", ackId);
  }
}

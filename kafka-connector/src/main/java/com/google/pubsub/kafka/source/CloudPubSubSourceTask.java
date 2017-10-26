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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.source.CloudPubSubSourceConnector.PartitionScheme;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
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
  private String cpsSubscription;
  private String kafkaMessageKeyAttribute;
  private int kafkaPartitions;
  private PartitionScheme kafkaPartitionScheme;
  private int cpsMaxBatchSize;
  // Keeps track of the current partition to publish to if the partition scheme is round robin.
  private int currentRoundRobinPartition = -1;
  // Keep track of all ack ids that have not been sent correctly acked yet.
  private Set<String> ackIds = Collections.synchronizedSet(new HashSet<String>());
  private CloudPubSubSubscriber subscriber;
  private Set<String> ackIdsInFlight = Collections.synchronizedSet(new HashSet<String>());

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
    cpsSubscription =
        String.format(
            ConnectorUtils.CPS_SUBSCRIPTION_FORMAT,
            validatedProps.get(ConnectorUtils.CPS_PROJECT_CONFIG).toString(),
            validatedProps.get(CloudPubSubSourceConnector.CPS_SUBSCRIPTION_CONFIG).toString());
    kafkaTopic = validatedProps.get(CloudPubSubSourceConnector.KAFKA_TOPIC_CONFIG).toString();
    cpsMaxBatchSize =
        (Integer) validatedProps.get(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG);
    kafkaPartitions =
        (Integer) validatedProps.get(CloudPubSubSourceConnector.KAFKA_PARTITIONS_CONFIG);
    kafkaMessageKeyAttribute =
        (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG);
    kafkaPartitionScheme =
        PartitionScheme.getEnum(
            (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG));
    if (subscriber == null) {
      // Only do this if we did not set through the constructor.
      subscriber = new CloudPubSubRoundRobinSubscriber(NUM_CPS_SUBSCRIBERS);
    }
    log.info("Started a CloudPubSubSourceTask.");
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.debug("Polling...");
    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription(cpsSubscription)
            .setReturnImmediately(false)
            .setMaxMessages(cpsMaxBatchSize)
            .build();
    try {
      PullResponse response = subscriber.pull(request).get();
      List<SourceRecord> sourceRecords = new ArrayList<>();
      log.trace("Received " + response.getReceivedMessagesList().size() + " messages");
      for (ReceivedMessage rm : response.getReceivedMessagesList()) {
        PubsubMessage message = rm.getMessage();
        String ackId = rm.getAckId();
        // If we are receiving this message a second (or more) times because the ack for it failed
        // then do not create a SourceRecord for this message. In case we are waiting for ack
        // response we also skip the message
        if (ackIds.contains(ackId) || ackIdsInFlight.contains(ackId)) {
          continue;
        }
        ackIds.add(ackId);
        Map<String, String> messageAttributes = message.getAttributes();
        String key = messageAttributes.get(kafkaMessageKeyAttribute);
        ByteString messageData = message.getData();
        byte[] messageBytes = messageData.toByteArray();

        boolean hasAttributes =
            messageAttributes.size() > 1 || (messageAttributes.size() > 0 && key == null);

        SourceRecord record = null;
        if (hasAttributes) {
          SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().field(
              ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD,
              Schema.BYTES_SCHEMA);

          for (Entry<String, String> attribute :
               messageAttributes.entrySet()) {
            if (!attribute.getKey().equals(kafkaMessageKeyAttribute)) {
              valueSchemaBuilder.field(attribute.getKey(),
                                       Schema.STRING_SCHEMA);
            }
          }

          Schema valueSchema = valueSchemaBuilder.build();
          Struct value =
              new Struct(valueSchema)
                  .put(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD,
                       messageBytes);
          for (Field field : valueSchema.fields()) {
            if (!field.name().equals(
                    ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD)) {
              value.put(field.name(), messageAttributes.get(field.name()));
            }
          }
          record =
            new SourceRecord(
                null,
                null,
                kafkaTopic,
                selectPartition(key, value),
                Schema.OPTIONAL_STRING_SCHEMA,
                key,
                valueSchema,
                value);
        } else {
          record =
            new SourceRecord(
                null,
                null,
                kafkaTopic,
                selectPartition(key, messageBytes),
                Schema.OPTIONAL_STRING_SCHEMA,
                key,
                Schema.BYTES_SCHEMA,
                messageBytes);
        }
        sourceRecords.add(record);
      }
      return sourceRecords;
    } catch (Exception e) {
      log.info("Error while retrieving records, treating as an empty poll. " + e);
      return new ArrayList<>();
    }
  }

  @Override
  public void commit() throws InterruptedException {
    ackMessages();
  }

  /**
   * Attempt to ack all ids in {@link #ackIds}.
   */
  private void ackMessages() {
    if (ackIds.size() != 0) {
      AcknowledgeRequest.Builder requestBuilder = AcknowledgeRequest.newBuilder()
          .setSubscription(cpsSubscription);
      final Set<String> ackIdsBatch = new HashSet<>();
      synchronized (ackIds) {
        requestBuilder.addAllAckIds(ackIds);
        ackIdsInFlight.addAll(ackIds);
        ackIdsBatch.addAll(ackIds);
        ackIds.clear();
      }
      ListenableFuture<Empty> response = subscriber.ackMessages(requestBuilder.build());
      Futures.addCallback(
          response,
          new FutureCallback<Empty>() {
            @Override
            public void onSuccess(Empty result) {
              ackIdsInFlight.removeAll(ackIdsBatch);
              log.trace("Successfully acked a set of messages.");
            }

            @Override
            public void onFailure(Throwable t) {
              ackIds.addAll(ackIdsBatch);
              ackIdsInFlight.removeAll(ackIdsBatch);
              log.error("An exception occurred acking messages: " + t);
            }
          });
    }
  }

  /** Return the partition a message should go to based on {@link #kafkaPartitionScheme}. */
  private int selectPartition(Object key, Object value) {
    if (kafkaPartitionScheme.equals(PartitionScheme.HASH_KEY)) {
      return key == null ? 0 : Math.abs(key.hashCode()) % kafkaPartitions;
    } else if (kafkaPartitionScheme.equals(PartitionScheme.HASH_VALUE)) {
      return Math.abs(value.hashCode()) % kafkaPartitions;
    } else {
      currentRoundRobinPartition = ++currentRoundRobinPartition % kafkaPartitions;
      return currentRoundRobinPartition;
    }
  }

  @Override
  public void stop() {}
}

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
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.common.ConnectorCredentialsProvider;
import com.google.pubsub.kafka.source.CloudPubSubSourceConnector.PartitionScheme;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Collections.synchronizedList;

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
  private String kafkaMessageTimestampAttribute;
  private int kafkaPartitions;
  private PartitionScheme kafkaPartitionScheme;
  private int cpsMaxBatchSize;
  private int maxAckBatchSize;
  private int maxMessagesInFlight;
  private long backOffPullIntervalMs;
  // Keeps track of the current partition to publish to if the partition scheme is round robin.
  private int currentRoundRobinPartition = -1;
  //Ack ids of messages that have been pulled but not yet delivered to kafka.
  private Set<String> ackIdsPulled = Collections.synchronizedSet(new HashSet<String>());
  // Ack ids of messages that have been delivered to kafka, but not attempted to be acknowledged in pubsub.
  private final Set<String> ackIdsOfMessagesDeliveredToKafka = Collections.synchronizedSet(new HashSet<String>());
  //Ack ids that are being sent to pubsub, but has not yet been acknowledged yet.
  private Set<String> ackIdsInFlight = Collections.synchronizedSet(new HashSet<String>());

  private final Set<String> standardAttributes = new HashSet<>();
  private CloudPubSubSubscriber subscriber;
  private ConnectorCredentialsProvider gcpCredentialsProvider;

  private final Object ackMutex = new Object();

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
    maxAckBatchSize = (Integer) validatedProps.get(CloudPubSubSourceConnector.MAX_ACK_BATCH_SIZE_CONFIG);
    maxMessagesInFlight = (Integer) validatedProps.get(CloudPubSubSourceConnector.MAX_MESSAGES_IN_FLIGHT_CONFIG);
    backOffPullIntervalMs = (Long) validatedProps.get(CloudPubSubSourceConnector.BACKOFF_PULL_INTERVAL_MS_CONFIG);
    kafkaPartitions =
        (Integer) validatedProps.get(CloudPubSubSourceConnector.KAFKA_PARTITIONS_CONFIG);
    kafkaMessageKeyAttribute =
        (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG);
    kafkaMessageTimestampAttribute =
        (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_MESSAGE_TIMESTAMP_CONFIG);
    kafkaPartitionScheme =
        PartitionScheme.getEnum(
            (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG));
    gcpCredentialsProvider = new ConnectorCredentialsProvider();
    String gcpCredentialsFilePath = (String) validatedProps.get(ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG);
    String credentialsJson = (String) validatedProps.get(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG);
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
    if (subscriber == null) {
      // Only do this if we did not set through the constructor.
      subscriber = new CloudPubSubRoundRobinSubscriber(NUM_CPS_SUBSCRIBERS, gcpCredentialsProvider);
    }
    standardAttributes.add(kafkaMessageKeyAttribute);
    standardAttributes.add(kafkaMessageTimestampAttribute);
    log.info("Started a CloudPubSubSourceTask.");
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    int numberOfInflightMessages  = getInFlightMessagesCount();
    if (numberOfInflightMessages > maxMessagesInFlight) {
      log.warn("Number of inflight messages is to high ({} > {}). Skip polling. Sleeping for {} ms.", numberOfInflightMessages, maxMessagesInFlight, backOffPullIntervalMs);
      Thread.sleep(backOffPullIntervalMs);
      return Collections.emptyList();
    }

    log.trace("Polling...");
    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription(cpsSubscription)
            .setReturnImmediately(false)
            .setMaxMessages(cpsMaxBatchSize)
            .build();
    try {
      PullResponse response = subscriber.pull(request).get();
      List<SourceRecord> sourceRecords = new ArrayList<>();
      log.debug("Received {} messages. Total number of inflight messages before pull: {}", response.getReceivedMessagesList().size(), numberOfInflightMessages);
      for (ReceivedMessage rm : response.getReceivedMessagesList()) {
        PubsubMessage message = rm.getMessage();
        String ackId = rm.getAckId();
        // If we are receiving this message a second (or more) times because the ack for it failed
        // then do not create a SourceRecord for this message. In case we are waiting for ack
        // response we also skip the message
        if (isAlreadyInFlight(ackId)) {
          continue;
        }
        ackIdsPulled.add(ackId);
        Map<String, String> messageAttributes = message.getAttributes();
        String key = messageAttributes.get(kafkaMessageKeyAttribute);
        Long timestamp = getLongValue(messageAttributes.get(kafkaMessageTimestampAttribute));
        if (timestamp == null){
          timestamp = Timestamps.toMillis(message.getPublishTime());
        }
        ByteString messageData = message.getData();
        byte[] messageBytes = messageData.toByteArray();

        boolean hasCustomAttributes = !standardAttributes.containsAll(messageAttributes.keySet());

        Map<String,String> ack = Collections.singletonMap(cpsSubscription, ackId);
        SourceRecord record = null;
        if (hasCustomAttributes) {
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
                ack,
                kafkaTopic,
                selectPartition(key, value),
                Schema.OPTIONAL_STRING_SCHEMA,
                key,
                valueSchema,
                value,
                timestamp);
        } else {
          record =
            new SourceRecord(
                null,
                ack,
                kafkaTopic,
                selectPartition(key, messageBytes),
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
      log.warn("Error while retrieving records, treating as an empty poll. " + e);
      return new ArrayList<>();
    }
  }

  /**
   * Returns number of messages that has been pulled, but not yet acknowledged.
   */
  int getInFlightMessagesCount() {
    return ackIdsPulled.size() + ackIdsOfMessagesDeliveredToKafka.size() + ackIdsInFlight.size();
  }

  /**
   * Checks if message has already ben delivered for processing. Does not guarantee duplicate detection,
   * since the message might not be in any of the collections being checked already.
   */
  boolean isAlreadyInFlight(String ackId) {
    return ackIdsPulled.contains(ackId) || ackIdsOfMessagesDeliveredToKafka.contains(ackId) || ackIdsInFlight.contains(ackId);
  }


  @Override
  public void commit() {
    ackMessages();
  }

  /**
   * Attempt to ack all ids in {@link #ackIdsOfMessagesDeliveredToKafka}.
   * Blocks until all ack requests succeed.
   */
  private void ackMessages() {
    if (ackIdsOfMessagesDeliveredToKafka.isEmpty())
      return;

    List<Set<String>> ackIdBatches;

    long startTimestamp = System.currentTimeMillis();
    int numberOfAcks;
    synchronized (ackMutex) {
      numberOfAcks = ackIdsOfMessagesDeliveredToKafka.size();
      ackIdBatches = split(ackIdsOfMessagesDeliveredToKafka, maxAckBatchSize);
      ackIdsInFlight.addAll(ackIdsOfMessagesDeliveredToKafka);
      ackIdsOfMessagesDeliveredToKafka.clear();
    }

    int ackAttempt = 0;
    while (!ackIdBatches.isEmpty()) {
      ack(ackIdBatches);
      if (!ackIdBatches.isEmpty()) {
        ++ackAttempt;
        log.warn("Have {} batches to ack after {} attempts.", ackIdBatches.size(), ackAttempt);
      }
    }

    log.debug("Have acked {} messages in {} ms", numberOfAcks, System.currentTimeMillis() - startTimestamp );
  }

  /**
   * Issues {@link AcknowledgeRequest} for each batch. Waits for all request to complete.
   * Removes successful batches from {@code ackBatches}
   */
  private void ack(final List<Set<String>> ackBatches) {
    List<ListenableFuture<Empty>> ackResponses = new ArrayList<>(ackBatches.size());

    final List<Set<String>> ackedBatches = synchronizedList(new ArrayList<Set<String>>());

    for (final Set<String> ackIdsBatch : ackBatches) {
      ListenableFuture<Empty> response = subscriber.ackMessages(
          AcknowledgeRequest.newBuilder()
              .setSubscription(cpsSubscription)
              .addAllAckIds(ackIdsBatch)
              .build()
      );


      Futures.addCallback(
          response,
          new FutureCallback<Empty>() {
            @Override
            public void onSuccess(Empty result) {
              ackedBatches.add(ackIdsBatch);
              log.trace("Successfully acked a set of messages of size {}.", ackIdsBatch.size());
            }

            @Override
            public void onFailure(Throwable t) {
              log.error("An exception occurred acking messages. Batch will be retried.", t);
            }
          }, directExecutor()
      );

      ackResponses.add(response);
    }

    try {
      Futures.whenAllComplete(ackResponses).call(
          new Callable<Empty>() {
            @Override
            public Empty call() {
              for (Set<String> ackedBatch : ackedBatches) {
                ackIdsInFlight.removeAll(ackedBatch);
              }
              ackBatches.removeAll(ackedBatches);
              log.debug("Successfully acked {} batches.", ackBatches.size());
              return Empty.getDefaultInstance();
            }
          },
          directExecutor()
      ).get();
    } catch (InterruptedException e) {
      log.warn("Got interrupted while waiting for acknowledgements.");
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed while acknowledging messages", e);
    }
  }

  static List<Set<String>> split(Collection<String> ackIds, int batchSize) {
      List<Set<String>> batches = new ArrayList<>();
      int numberOfBatches = (ackIds.size() + batchSize -1) / batchSize ;

      for (int i = 0; i < numberOfBatches; i++) {
        batches.add(new HashSet<String>());
      }

      int idx = 0;
      for (String ackId : ackIds) {
        batches.get(idx ++ / batchSize).add(ackId);
      }
      return batches;

  }

  /** Return the partition a message should go to based on {@link #kafkaPartitionScheme}. */
  private Integer selectPartition(Object key, Object value) {
    if (kafkaPartitionScheme.equals(PartitionScheme.HASH_KEY)) {
      return key == null ? 0 : Math.abs(key.hashCode()) % kafkaPartitions;
    } else if (kafkaPartitionScheme.equals(PartitionScheme.HASH_VALUE)) {
      return Math.abs(value.hashCode()) % kafkaPartitions;
    } if (kafkaPartitionScheme.equals(PartitionScheme.KAFKA_PARTITIONER)) {
      return null;
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
  public void stop() {}

  @Override
  public void commitRecord(SourceRecord record) {
    String ackId = record.sourceOffset().get(cpsSubscription).toString();
    synchronized (ackMutex) {
      ackIdsOfMessagesDeliveredToKafka.add(ackId);
      ackIdsPulled.remove(ackId);
    }
    log.trace("Committed {}", ackId);
  }

}

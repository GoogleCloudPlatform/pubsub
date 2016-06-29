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
package com.google.pubsub.kafka;

import com.google.api.client.util.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/***
 * A {@link SinkTask} used by a {@link CloudPubSubSinkConnector} to write messages to
 * <a href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public class CloudPubSubSinkTask extends SinkTask {
  private static final String SCHEMA_NAME = ByteString.class.getName();
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSinkTask.class);

  private static final int NUM_PUBLISHERS = 10;
  private static final int MAX_REQUEST_SIZE = (10<<20) - 1024; // Leave a little room for overhead.
  private static final int MAX_MESSAGES_PER_REQUEST = 1000;
  private static final String KEY_ATTRIBUTE = "key";
  private static final int KEY_ATTRIBUTE_SIZE = KEY_ATTRIBUTE.length();
  private static final String PARTITION_ATTRIBUTE = "partition";
  private static final int PARTITION_ATTRIBUTE_SIZE = PARTITION_ATTRIBUTE.length();
  private static final String TOPIC_FORMAT = "projects/%s/topics/%s";

  private class UnpublishedMessages {
    public List<PubsubMessage> messages = new ArrayList<>();
    public int size = 0;
  }

  private String cpsTopic;
  private int minBatchSize;
  private Map<Integer, List<ListenableFuture<PublishResponse>>> outstandingPublishes =
      Maps.newHashMap();
  private Map<Integer, UnpublishedMessages> unpublishedMessages = Maps.newHashMap();
  private CloudPubSubPublisher publisher;

  public CloudPubSubSinkTask() {}

  @Override
  public String version() {
    return new CloudPubSubSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    this.cpsTopic =
        String.format(
            TOPIC_FORMAT,
            props.get(CloudPubSubSinkConnector.CPS_PROJECT_CONFIG),
            props.get(CloudPubSubSinkConnector.CPS_TOPIC_CONFIG));
    this.minBatchSize = Integer.parseInt(props.get(CloudPubSubSinkConnector.CPS_MIN_BATCH_SIZE));
    log.info("Start connector task for topic " + cpsTopic + " min batch size = " + minBatchSize);
    this.publisher = new CloudPubSubRoundRobinPublisher(NUM_PUBLISHERS);
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    log.debug("Received " + sinkRecords.size() + " messages to send to CPS.");
    PubsubMessage.Builder builder = PubsubMessage.newBuilder();
    for (SinkRecord record : sinkRecords) {
      if (record.valueSchema().type() != Schema.Type.BYTES ||
          !record.valueSchema().name().equals(SCHEMA_NAME)) {
        throw new DataException("Unexpected record of type " + record.valueSchema());
      }
      log.trace("Received record: " + record.toString());

      final Map<String, String> attributes = Maps.newHashMap();
      ByteString value = (ByteString)record.value();
      String key = "";
      String partition = "";
      if (record.key() != null) {
        key = record.key().toString();
        attributes.put(KEY_ATTRIBUTE, key.toString());
      }
      if (record.kafkaPartition() != null) {
        partition = record.kafkaPartition().toString();
        attributes.put(PARTITION_ATTRIBUTE, partition.toString());
      }
      PubsubMessage message = builder
          .setData(value)
          .putAllAttributes(attributes)
          .build();
      builder.clear();

      UnpublishedMessages messagesForPartition = unpublishedMessages.get(record.kafkaPartition());
      if (messagesForPartition == null) {
        messagesForPartition = new UnpublishedMessages();
        unpublishedMessages.put(record.kafkaPartition(), messagesForPartition);
      }

      int messageSize = key.length() + partition.length() + value.size() +
                        KEY_ATTRIBUTE_SIZE + PARTITION_ATTRIBUTE_SIZE;
      int newUnpublishedSize = messagesForPartition.size + messageSize;
      if (newUnpublishedSize > MAX_REQUEST_SIZE) {
        publishMessagesForPartition(record.kafkaPartition(), messagesForPartition.messages);
        newUnpublishedSize = messageSize;
        messagesForPartition.messages.clear();
      }

      messagesForPartition.size = newUnpublishedSize;
      messagesForPartition.messages.add(message);

      if (messagesForPartition.messages.size() >= minBatchSize) {
        publishMessagesForPartition(record.kafkaPartition(), messagesForPartition.messages);
        unpublishedMessages.remove(record.kafkaPartition());
      }
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> partitionOffsets) {
    for (Map.Entry<Integer, UnpublishedMessages> messagesForPartition :
        unpublishedMessages.entrySet()) {
      publishMessagesForPartition(messagesForPartition.getKey(),
                                  messagesForPartition.getValue().messages);
    }
    unpublishedMessages.clear();

    for (Map.Entry<TopicPartition, OffsetAndMetadata> partitionOffset :
        partitionOffsets.entrySet()) {
      log.debug("Received flush for partition " + partitionOffset.getKey().toString());
      List<ListenableFuture<PublishResponse>> outstandingPublishesForPartition =
          outstandingPublishes.get(partitionOffset.getKey().partition());
      if (outstandingPublishesForPartition == null) {
        continue;
      }

      try {
        for (ListenableFuture<PublishResponse> publishRequest : outstandingPublishesForPartition) {
          publishRequest.get();
        }
        outstandingPublishes.remove(partitionOffset.getKey().partition());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void stop() {}

  private void publishMessagesForPartition(Integer partition, List<PubsubMessage> messages) {
    List<ListenableFuture<PublishResponse>> outstandingPublishesForPartition =
        outstandingPublishes.get(partition);
    if (outstandingPublishesForPartition == null) {
      outstandingPublishesForPartition = new ArrayList<>();
      outstandingPublishes.put(partition, outstandingPublishesForPartition);
    }

    int startIndex = 0;
    int endIndex = Math.min(MAX_MESSAGES_PER_REQUEST, messages.size());

    PublishRequest.Builder builder = PublishRequest.newBuilder();
    while (startIndex < messages.size()) {
      PublishRequest request = builder
          .setTopic(cpsTopic)
          .addAllMessages(messages.subList(startIndex, endIndex))
          .build();
      builder.clear();
      // log.info("Publishing: " + (endIndex - startIndex) + " messages");
      outstandingPublishesForPartition.add(publisher.publish(request));
      startIndex = endIndex;
      endIndex = Math.min(endIndex + MAX_MESSAGES_PER_REQUEST, messages.size());
    }
  }
}

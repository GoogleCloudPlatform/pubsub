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
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * A {@link SourceTask} used by a {@link CloudPubSubSourceConnector} to write messages to
 * <a href="http://kafka.apache.org/">Apache Kafka</a>.
 */
public class CloudPubSubSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSourceTask.class);

  protected static final int NUM_SUBSCRIBERS = 10;

  protected String cpsTopic;
  protected int maxBatchSize;
  protected CloudPubSubSubscriber subscriber;
  protected String subscriptionName;

  @Override
  public String version() {
    return new CloudPubSubSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    cpsTopic =
        String.format(
            ConnectorUtils.CPS_TOPIC_FORMAT,
            props.get(ConnectorUtils.CPS_PROJECT_CONFIG),
            props.get(ConnectorUtils.CPS_TOPIC_CONFIG));
    maxBatchSize = Integer.parseInt(props.get(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG));
    log.info("Start connector task for topic " + cpsTopic + " max batch size = " + maxBatchSize);
    subscriber = new CloudPubSubRoundRobinSubscriber(NUM_SUBSCRIBERS);
    subscriptionName = props.get(CloudPubSubSourceConnector.SUBSCRIPTION_NAME);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    PullRequest request = PullRequest.newBuilder()
        .setSubscription(subscriptionName)
        .setReturnImmediately(false)
        .setMaxMessages(maxBatchSize)
        .build();
    try {
      PullResponse response = subscriber.pull(request).get();
      // Stores ackIds for all received messages.
      List<String> ackIds = new ArrayList<>();
      List<SourceRecord> sourceRecords = new ArrayList<>();
      for (ReceivedMessage rm : response.getReceivedMessagesList()) {
        PubsubMessage message = rm.getMessage();
        ackIds.add(rm.getAckId());
        // Get the message attributes and parse out the relevant ones.
        Map<String, String> messageAttributes = message.getAttributes();
        Integer partition = 0;
        if (messageAttributes.get(ConnectorUtils.PARTITION_ATTRIBUTE) != null) {
          partition = Integer.parseInt(messageAttributes.get(ConnectorUtils.PARTITION_ATTRIBUTE));
        }
        String topic = cpsTopic;
        if (messageAttributes.get(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE) != null) {
          topic = messageAttributes.get(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE);
        }
        String key = messageAttributes.get(ConnectorUtils.KEY_ATTRIBUTE);
        // We don't need to check that the message data is a byte string because we know the
        // data is coming from CPS so it must be of that type.
        SourceRecord record = new SourceRecord(
            null,
            null,
            topic,
            partition,
            SchemaBuilder.string().build(),
            key,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            message.getData());
        sourceRecords.add(record);
      }
      ackMessages(ackIds);
      return sourceRecords;
    } catch (Exception e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  @VisibleForTesting
  protected void ackMessages(List<String> ackIds) {
    try {
      AcknowledgeRequest request = AcknowledgeRequest.newBuilder()
          .setSubscription(subscriptionName)
          .addAllAckIds(ackIds)
          .build();
      subscriber.ackMessages(request).get();
    } catch (Exception e) {
      log.error("An exception occurred acking messages. Unacked messages will be resent.");
    }
  }

  @Override
  public void stop() {
    // TODO(rramkumar): Find out how to implement this.
  }
}

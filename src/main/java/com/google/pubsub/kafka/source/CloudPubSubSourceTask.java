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

import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.sink.CloudPubSubSinkConnector;
import com.google.pubsub.kafka.sink.CloudPubSubSinkTask;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.AcknowledgeRequest;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/***
 * A {@link SourceTask} used by a {@link CloudPubSubSourceConnector} to write messages to Kafka.
 */
class CloudPubSubSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSinkTask.class);

  private String cpsTopic;
  private int maxBatchSize;
  private CloudPubSubSubscriber subscriber;
  private Subscription subscription;

  public CloudPubSubSourceTask() {}

  @Override
  public String version() {
    return new CloudPubSubSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    this.cpsTopic =
        String.format(
            ConnectorUtils.TOPIC_FORMAT,
            props.get(CloudPubSubSinkConnector.CPS_PROJECT_CONFIG),
            props.get(CloudPubSubSinkConnector.CPS_TOPIC_CONFIG));
    this.maxBatchSize = Integer.parseInt(props.get(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE));
    log.info("Start connector task for topic " + cpsTopic + " max batch size = " + maxBatchSize);
    this.subscriber = new CloudPubSubRoundRobinSubscriber(10);
    try {
      SubscriberFutureStub stub = SubscriberGrpc.newFutureStub(ConnectorUtils.getChannel());
      Subscription request = Subscription.newBuilder().setTopic(cpsTopic).build();
      subscription = stub.createSubscription(request).get();
    } catch (Exception e) {
      throw new RuntimeException("Could not subscribe to the specified CPS topic: " + e);
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    PullRequest request = PullRequest.newBuilder()
        .setSubscription(subscription.getName())
        .setReturnImmediately(true)
        .setMaxMessages(maxBatchSize)
        .build();
    try {
      PullResponse response = subscriber.pull(request).get();
      List<String> ackIds = new ArrayList<>();
      List<SourceRecord> sourceRecords = new ArrayList<>();
      for (ReceivedMessage rm : response.getReceivedMessagesList()) {
        PubsubMessage message = rm.getMessage();
        ackIds.add(rm.getAckId());
        // TODO(rramkumar): Revisit attribute issue.
        Map<String, String> messageAttributes = message.getAttributes();
        Integer partition = null;
        if (messageAttributes.get(ConnectorUtils.PARTITION_ATTRIBUTE) != null) {
          partition = Integer.parseInt(messageAttributes.get(ConnectorUtils.PARTITION_ATTRIBUTE));
        }
        String key = messageAttributes.get(ConnectorUtils.KEY_ATTRIBUTE);
        SourceRecord record = new SourceRecord(
            null,
            null,
            messageAttributes.get(ConnectorUtils.KAFKA_TOPIC_ATTRIBUTE),
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
 
  private void ackMessages(List<String> ackIds) throws Exception{
    AcknowledgeRequest request = AcknowledgeRequest.newBuilder()
        .setSubscription(subscription.getName())
        .addAllAckIds(ackIds)
        .build();
    subscriber.ackMessages(request).get();
  }

  @Override
  public void stop() {}
}

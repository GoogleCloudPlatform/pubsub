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

import com.google.pubsub.kafka.sink.CloudPubSubSinkConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

class CloudPubSubSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSinkTask.class);
  private static final String SCHEMA_NAME = ByteString.class.getName();
  private static final int NUM_PUBLISHERS = 10;
  private static final int MAX_REQUEST_SIZE = (10<<20) - 1024; // Leave a little room for overhead.
  private static final int MAX_MESSAGES_PER_REQUEST = 1000;
  private static final String KEY_ATTRIBUTE = "key";
  private static final int KEY_ATTRIBUTE_SIZE = KEY_ATTRIBUTE.length();
  private static final String PARTITION_ATTRIBUTE = "partition";
  private static final int PARTITION_ATTRIBUTE_SIZE = PARTITION_ATTRIBUTE.length();
  private static final String KAFKA_TOPIC_ATTRIBUTE = "kafka_topic";
  private static final int KAFKA_TOPIC_ATTRIBUTE_SIZE = KAFKA_TOPIC_ATTRIBUTE.length();
  private static final String TOPIC_FORMAT = "projects/%s/topics/%s";

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
  public void start(Map<String, String> map) {
    this.cpsTopic =
        String.format(
            TOPIC_FORMAT,
            props.get(CloudPubSubSinkConnector.CPS_PROJECT_CONFIG),
            props.get(CloudPubSubSinkConnector.CPS_TOPIC_CONFIG));
    this.maxBatchSize = props.get(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE);
    log.info("Start connector task for topic " + cpsTopic + " min batch size = " + minBatchSize);
    this.subscriber = new CloudPubSubGRPCSubscriber();
    try {
      Subscription s = Subscription.newBuilder().setTopic(cpsTopic).build();
      subscription = subscriber.subscribe(s).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Could not subscribe to the specified CPS topic: " + e);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    PullRequest request = PullRequest.newBuilder()
        .setSubscription(subscription.name())
        .returnImmediately(false)
        .setMaxMessages(maxBatchSize)
        .build();
    PullResponse response = subscriber.pull(request).get();
    List<String> ackIds = new ArrayList<>();
    List<SourceRecord> sourceRecords = new ArrayList<>();
    for (ReceivedMessage rm : response.receivedMessages()) {
      PubsubMessage message = rm.message();
      ackIds.add(rm.ackId());
    }
    ackMessages(ackIds);

    // Pull a batch of messages and ack these messages.
    // Create a SourceRecord for each message in the batch and return a list of SourceRecord objects
    //
  }
  
  private void ackMessages(List<String> ackIds) {
  }
  @Override
  public void stop() {}
}

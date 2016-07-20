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

import org.mockito.Mockito;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link CloudPubSubSourceTask}. */
public class CloudPubSubSourceTaskTest {

  private static final String CPS_TOPIC = "the";
  private static final String CPS_PROJECT = "quick";
  private static final String CPS_MAX_BATCH_SIZE = "1000";
  private static final String CPS_SUBSCRIPTION = "brown";
  private static final String KAFKA_TOPIC = "fox";
  private static final String KAFKA_MESSAGE_KEY = "jumped";
  private static final String MESSAGE = "over";


  private CloudPubSubSourceTask sourceTask;
  private Map<String, String> taskProps;

  @Before
  public void setup() {
    sourceTask = spy(new CloudPubSubSourceTask());
    taskProps = new HashMap<>();
    taskProps.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
    taskProps.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    taskProps.put(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG, CPS_MAX_BATCH_SIZE);
    taskProps.put(CloudPubSubSourceConnector.CPS_SUBSCRIPTION_CONFIG, CPS_SUBSCRIPTION);
    taskProps.put(CloudPubSubSourceConnector.KAFKA_TOPIC_CONFIG, KAFKA_TOPIC);
    taskProps.put(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG, KAFKA_MESSAGE_KEY);
  }

  @Test
  public void testStart() {
    sourceTask.start(taskProps);
    assertEquals(
        sourceTask.cpsTopic,
        String.format(ConnectorUtils.CPS_TOPIC_FORMAT, CPS_PROJECT, CPS_TOPIC));
    assertEquals(sourceTask.maxBatchSize, Integer.parseInt(CPS_MAX_BATCH_SIZE));
    assertEquals(sourceTask.cpsSubscription, CPS_SUBSCRIPTION);
    assertEquals(sourceTask.kafkaTopic, KAFKA_TOPIC);
    assertEquals(sourceTask.keyAttribute, KAFKA_MESSAGE_KEY);
  }

  /** Tests when no messages are received from the CPS PullResponse. */
  @Test
  public void testPollCase1() throws Exception {
    setupSourceTaskManually();
    PullResponse stubResponse = PullResponse.newBuilder().build();
    when(sourceTask.subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubResponse);
    assertEquals(0, sourceTask.poll().size());
  }

  /**
   * Tests when the message(s) retrieved from CPS does not have an attribute that
   * matches {@link #KAFKA_MESSAGE_KEY}
   */
  @Test
  public void testPollCase2() throws Exception {
    setupSourceTaskManually();
    Map<String, String> messageAttributes = new HashMap<>();
    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(MESSAGE))
            .putAllAttributes(messageAttributes)
            .build();
    ReceivedMessage rm = ReceivedMessage.newBuilder().setMessage(message).build();
    PullResponse stubResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(sourceTask.subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubResponse);
    List<SourceRecord> result = sourceTask.poll();
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            SchemaBuilder.string().build(),
            null,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            ByteString.copyFromUtf8(MESSAGE));
    assertEquals(expected, result.get(0));
  }

  /**
   * Tests when the message(s) retrieved from CPS does have an attribute that matches
   * {@link #KAFKA_MESSAGE_KEY}
   */
  @Test
  public void testPollCase3() throws Exception {
    setupSourceTaskManually();
    Map<String, String> messageAttributes = new HashMap<>();
    messageAttributes.put(ConnectorUtils.KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY);
    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(MESSAGE))
            .putAllAttributes(messageAttributes)
            .build();
    ReceivedMessage rm = ReceivedMessage.newBuilder().setMessage(message).build();
    PullResponse stubResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(sourceTask.subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubResponse);
    List<SourceRecord> result = sourceTask.poll();
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            SchemaBuilder.string().build(),
            KAFKA_MESSAGE_KEY,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            ByteString.copyFromUtf8(MESSAGE));
    assertEquals(expected, result.get(0));
  }

  @Test(expected = InterruptedException.class)
  public void testPollExceptionCase1() throws Exception {
    setupSourceTaskManually();
    // Could also throw ExecutionException...
    when(sourceTask.subscriber.pull(any(PullRequest.class)).get())
        .thenThrow(new InterruptedException());
    sourceTask.poll();
  }

  /** Performs the setup for the task without calling start(). */
  private void setupSourceTaskManually() {
    doNothing().when(sourceTask).ackMessages(anyList());
    sourceTask.cpsTopic = String.format(ConnectorUtils.CPS_TOPIC_FORMAT, CPS_PROJECT, CPS_TOPIC);
    sourceTask.maxBatchSize = Integer.parseInt(CPS_MAX_BATCH_SIZE);
    sourceTask.cpsSubscription = CPS_SUBSCRIPTION;
    sourceTask.subscriber = mock(CloudPubSubSubscriber.class, Mockito.RETURNS_DEEP_STUBS);
    sourceTask.kafkaTopic = KAFKA_TOPIC;
    sourceTask.keyAttribute = KAFKA_MESSAGE_KEY;
  }
}

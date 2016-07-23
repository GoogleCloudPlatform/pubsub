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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.source.CloudPubSubSourceConnector;
import com.google.pubsub.kafka.source.CloudPubSubSourceTask;
import com.google.pubsub.kafka.source.CloudPubSubSubscriber;
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
import org.mockito.Mockito;

/** Tests for {@link CloudPubSubSourceTask}. */
public class CloudPubSubSourceTaskTest {

  private static final String CPS_TOPIC = "the";
  private static final String CPS_PROJECT = "quick";
  private static final String CPS_MAX_BATCH_SIZE = "1000";
  private static final String CPS_SUBSCRIPTION = "brown";
  private static final String KAFKA_TOPIC = "fox";
  private static final String KAFKA_MESSAGE_KEY_ATTRIBUTE = "jumped";
  private static final String KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE = "over";
  private static final String KAFKA_PARTITIONS = "3";
  private static final String CPS_MESSAGE = "lazy";
  private static final String ACK_ID1 = "ackID1";
  private static final String ACK_ID2 = "ackID2";

  private CloudPubSubSourceTask sourceTask;
  private Map<String, String> taskProps;

  @Before
  public void setup() {
    sourceTask = spy(new CloudPubSubSourceTask());
    sourceTask.subscriber = mock(CloudPubSubSubscriber.class);
    taskProps = new HashMap<>();
    taskProps.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
    taskProps.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    taskProps.put(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG, CPS_MAX_BATCH_SIZE);
    taskProps.put(CloudPubSubSourceConnector.CPS_SUBSCRIPTION_CONFIG, CPS_SUBSCRIPTION);
    taskProps.put(CloudPubSubSourceConnector.KAFKA_TOPIC_CONFIG, KAFKA_TOPIC);
    taskProps.put(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG, KAFKA_MESSAGE_KEY_ATTRIBUTE);
    taskProps.put(CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        CloudPubSubSourceConnector.PartitionScheme.ROUND_ROBIN.toString())
  }

  /**
   * Tests when no messages are received from the Cloud Pub/Sub PullResponse.
   */
  @Test
  public void testPollCase1() throws Exception {
    sourceTask.start(taskProps);
    doNothing().when(sourceTask).ackMessages();
    PullResponse stubbedPullResponse = PullResponse.newBuilder().build();
    when(sourceTask.subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    assertEquals(0, sourceTask.poll().size());
  }

  /**
   * Tests that when a call to ackMessages() fails, that the message is not sent again to Kafka if
   * the message is received again by Cloud Pub/Sub. Also tests that ack ids are added properly
   * if the ack id has not been seen before. Implicitly test that
   */
  @Test
  public void testPollCase2() throws Exception {
    sourceTask.start(taskProps);
    sourceTask.ackIds.add(ACK_ID1);
    // Doing this mimics the situation when sending the acks fails.
    doNothing().when(sourceTask).ackMessages();
    // Mimics how Cloud Pub/Sub will resend messages if they were not acked properly.
    ReceivedMessage rm1 = ReceivedMessage.newBuilder().setAckId(ACK_ID1).build();
    ReceivedMessage rm2 = ReceivedMessage.newBuilder().setAckId(ACK_ID2).build();
    PullResponse stubbedPullResponse = PullResponse.newBuilder()
        .addReceivedMessages(rm1)
        .addReceivedMessages(rm2)
        .build();
    when(sourceTask.subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = sourceTask.poll();
    assertEquals(1, result.size());
    assertTrue(sourceTask.ackIds.contains(ACK_ID2));
    assertTrue(sourceTask.ackIds.contains(ACK_ID1));
  }

  /**
   * Tests when the message(s) retrieved from Cloud Pub/Sub do not have an attribute that matches
   * {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollCase3() throws Exception {
    ByteString messageByteString = ByteString.copyFromUtf8(CPS_MESSAGE);
    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(messageByteString)
            .putAllAttributes(new HashMap<>())
            .build();
    ReceivedMessage rm = ReceivedMessage.newBuilder().setMessage(message).build();
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(sourceTask.subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
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
            messageByteString);
    assertEquals(expected, result.get(0));
  }

  /**
   * Tests when the message(s) retrieved from Cloud Pub/Sub do have an attribute that
   * matches {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollCase4() throws Exception {
    sourceTask.start(taskProps);
    ByteString messageByteString = ByteString.copyFromUtf8(CPS_MESSAGE);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(messageByteString)
            .putAllAttributes(attributes)
            .build();
    ReceivedMessage rm = ReceivedMessage.newBuilder().setMessage(message).build();
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(sourceTask.subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    List<SourceRecord> result = sourceTask.poll();
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            SchemaBuilder.string().build(),
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            messageByteString);
    assertEquals(expected, result.get(0));
  }

  /**
   * Tests that partitions are properly assigned when the partition scheme matches
   * is {@link PartitionScheme.ROUND_ROBIN}
   */
  @Test
  public void testPollCase5() throws Exception {
    sourceTask.start(taskProps);
    ByteString messageByteString = ByteString.copyFromUtf8(CPS_MESSAGE);
    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(messageByteString)
            .putAllAttributes(new HashMap<>())
            .build();
    ReceivedMessage rm = ReceivedMessage.newBuilder().setMessage(message).build();
    PullResponse stubResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(sourceTask.subscriber.pull(any(PullRequest.class)).get()).thenReturn(stubResponse);
    List<SourceRecord> result = sourceTask.poll();
    assertEquals(4, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            SchemaBuilder.string().build(),
            null,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            messageByteString);
    assertEquals(expected, result.get(0));
  }

  @Test(expected = InterruptedException.class)
  public void testPollExceptionCase() throws Exception {
    // Could also throw ExecutionException if we wanted to...
    when(sourceTask.subscriber.pull(any(PullRequest.class)).get())
        .thenThrow(new InterruptedException());
    sourceTask.poll();
  }
}

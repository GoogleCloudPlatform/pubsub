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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.AcknowledgeRequest;
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
  private static final String KAFKA_MESSAGE_KEY_ATTRIBUTE = "jumped";
  private static final String KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE = "over";
  private static final String KAFKA_PARTITIONS = "3";
  private static final ByteString CPS_MESSAGE = ByteString.copyFromUtf8("lazy");
  private static final String ACK_ID1 = "ackID1";
  private static final String ACK_ID2 = "ackID2";
  private static final String ACK_ID3 = "ackID3";
  private static final String ACK_ID4 = "ackID4";

  private CloudPubSubSourceTask task;
  private Map<String, String> props;

  @Before
  public void setup() {
    task = spy(new CloudPubSubSourceTask(mock(CloudPubSubSubscriber.class, RETURNS_DEEP_STUBS)));
    props = new HashMap<>();
    props.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
    props.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    props.put(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG, CPS_MAX_BATCH_SIZE);
    props.put(CloudPubSubSourceConnector.CPS_SUBSCRIPTION_CONFIG, CPS_SUBSCRIPTION);
    props.put(CloudPubSubSourceConnector.KAFKA_TOPIC_CONFIG, KAFKA_TOPIC);
    props.put(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG, KAFKA_MESSAGE_KEY_ATTRIBUTE);
    props.put(CloudPubSubSourceConnector.KAFKA_PARTITIONS_CONFIG, KAFKA_PARTITIONS);
    props.put(
        CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        CloudPubSubSourceConnector.PartitionScheme.ROUND_ROBIN.toString());
  }


  /** Tests when no messages are received from the Cloud Pub/Sub PullResponse. */
  @Test
  public void testPollCaseWithNoMessages() throws Exception {
    task.start(props);
    PullResponse stubbedPullResponse = PullResponse.newBuilder().build();
    when(task.getSubscriber().pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    ListenableFuture<Empty> goodFuture = Futures.immediateFuture(Empty.getDefaultInstance());
    when(task.getSubscriber().ackMessages(any(AcknowledgeRequest.class))).thenReturn(goodFuture);
    assertEquals(0, task.poll().size());
  }

  /**
   * Tests that when a call to ackMessages() fails, that the message is not sent again to Kafka if
   * the message is received again by Cloud Pub/Sub. Also tests that ack ids are added properly if
   * the ack id has not been seen before.
   */
  @Test
  public void testPollWithDuplicateReceivedMessages() throws Exception {
    task.start(props);
    ReceivedMessage rm1 = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>());
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm1).build();
    when(task.getSubscriber().pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    ListenableFuture<Empty> failedFuture = Futures.immediateFailedFuture(new Throwable());
    when(task.getSubscriber().ackMessages(any(AcknowledgeRequest.class))).thenReturn(failedFuture);
    List<SourceRecord> result = task.poll();
    assertEquals(1, result.size());
    ReceivedMessage rm2 = createReceivedMessage(ACK_ID2, CPS_MESSAGE, new HashMap<>());
    stubbedPullResponse =
        PullResponse.newBuilder().addReceivedMessages(0, rm1).addReceivedMessages(1, rm2).build();
    when(task.getSubscriber().pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    ListenableFuture<Empty> goodFuture = Futures.immediateFuture(Empty.getDefaultInstance());
    when(task.getSubscriber().ackMessages(any(AcknowledgeRequest.class))).thenReturn(goodFuture);
    result = task.poll();
    assertEquals(1, result.size());
  }

  /**
   * Tests when the message(s) retrieved from Cloud Pub/Sub do not have an attribute that matches
   * {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollWithNoMessageKeyAttribute() throws Exception {
    task.start(props);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>());
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(task.getSubscriber().pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    ListenableFuture<Empty> goodFuture = Futures.immediateFuture(Empty.getDefaultInstance());
    when(task.getSubscriber().ackMessages(any(AcknowledgeRequest.class))).thenReturn(goodFuture);
    List<SourceRecord> result = task.poll();
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
            CPS_MESSAGE);
    assertEquals(expected, result.get(0));
  }

  /**
   * Tests when the message(s) retrieved from Cloud Pub/Sub do have an attribute that matches {@link
   * #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollWithMessageKeyAttribute() throws Exception {
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, attributes);
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(task.getSubscriber().pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    ListenableFuture<Empty> goodFuture = Futures.immediateFuture(Empty.getDefaultInstance());
    when(task.getSubscriber().ackMessages(any(AcknowledgeRequest.class))).thenReturn(goodFuture);
    List<SourceRecord> result = task.poll();
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
            CPS_MESSAGE);
    assertEquals(expected, result.get(0));
  }

  /**
   * Tests that the correct partition is assigned when the partition scheme is "hash_key". The test
   * has two cases, one where a key does exist and one where it does not.
   */
  @Test
  public void testPollWithPartitionSchemeHashKey() throws Exception {
    props.put(
        CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        CloudPubSubSourceConnector.PartitionScheme.HASH_KEY.toString());
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    ReceivedMessage withoutKey = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>());
    ReceivedMessage withKey = createReceivedMessage(ACK_ID2, CPS_MESSAGE, attributes);
    PullResponse stubbedPullResponse =
        PullResponse.newBuilder()
            .addReceivedMessages(0, withKey)
            .addReceivedMessages(1, withoutKey)
            .build();
    when(task.getSubscriber().pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    ListenableFuture<Empty> goodFuture = Futures.immediateFuture(Empty.getDefaultInstance());
    when(task.getSubscriber().ackMessages(any(AcknowledgeRequest.class))).thenReturn(goodFuture);
    List<SourceRecord> result = task.poll();
    assertEquals(2, result.size());
    SourceRecord expectedForMessageWithKey =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE.hashCode() % Integer.parseInt(KAFKA_PARTITIONS),
            SchemaBuilder.string().build(),
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            CPS_MESSAGE);
    SourceRecord expectedForMessageWithoutKey =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            SchemaBuilder.string().build(),
            null,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            CPS_MESSAGE);
    assertEquals(expectedForMessageWithKey, result.get(0));
    assertEquals(expectedForMessageWithoutKey, result.get(1));
  }

  /** Tests that the correct partition is assigned when the partition scheme is "hash_value". */
  @Test
  public void testPollWithPartitionSchemeHashValue() throws Exception {
    props.put(
        CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        CloudPubSubSourceConnector.PartitionScheme.HASH_VALUE.toString());
    task.start(props);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>());
    PullResponse stubbedPullResponse = PullResponse.newBuilder().addReceivedMessages(rm).build();
    when(task.getSubscriber().pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    ListenableFuture<Empty> goodFuture = Futures.immediateFuture(Empty.getDefaultInstance());
    when(task.getSubscriber().ackMessages(any(AcknowledgeRequest.class))).thenReturn(goodFuture);
    List<SourceRecord> result = task.poll();
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            CPS_MESSAGE.hashCode() % Integer.parseInt(KAFKA_PARTITIONS),
            SchemaBuilder.string().build(),
            null,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            CPS_MESSAGE);
    assertEquals(expected, result.get(0));
  }

  /**
   * Tests that the correct partition is assigned when the partition scheme is "round_robin". The
   * tests makes sure to submit an approrpriate number of messages to poll() so that all partitions
   * in the round robin are hit once.
   */
  @Test
  public void testPollWithPartitionSchemeRoundRobin() throws Exception {
    task.start(props);
    ReceivedMessage rm1 = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>());
    ReceivedMessage rm2 = createReceivedMessage(ACK_ID2, CPS_MESSAGE, new HashMap<>());
    ReceivedMessage rm3 = createReceivedMessage(ACK_ID3, CPS_MESSAGE, new HashMap<>());
    ReceivedMessage rm4 = createReceivedMessage(ACK_ID4, CPS_MESSAGE, new HashMap<>());
    PullResponse stubbedPullResponse =
        PullResponse.newBuilder()
            .addReceivedMessages(0, rm1)
            .addReceivedMessages(1, rm2)
            .addReceivedMessages(2, rm3)
            .addReceivedMessages(3, rm4)
            .build();
    when(task.getSubscriber().pull(any(PullRequest.class)).get()).thenReturn(stubbedPullResponse);
    ListenableFuture<Empty> goodFuture = Futures.immediateFuture(Empty.getDefaultInstance());
    when(task.getSubscriber().ackMessages(any(AcknowledgeRequest.class))).thenReturn(goodFuture);
    List<SourceRecord> result = task.poll();
    assertEquals(4, result.size());
    SourceRecord expected1 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            SchemaBuilder.string().build(),
            null,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            CPS_MESSAGE);
    SourceRecord expected2 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            1,
            SchemaBuilder.string().build(),
            null,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            CPS_MESSAGE);
    SourceRecord expected3 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            2,
            SchemaBuilder.string().build(),
            null,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            CPS_MESSAGE);
    SourceRecord expected4 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            SchemaBuilder.string().build(),
            null,
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            CPS_MESSAGE);
    assertEquals(expected1, result.get(0));
    assertEquals(expected2, result.get(1));
    assertEquals(expected3, result.get(2));
    assertEquals(expected4, result.get(3));
  }

  @Test(expected = InterruptedException.class)
  public void testPollExceptionCase() throws Exception {
    task.start(props);
    // Could also throw ExecutionException if we wanted to...
    when(task.getSubscriber().pull(any(PullRequest.class)).get())
        .thenThrow(new InterruptedException());
    task.poll();
  }

  private ReceivedMessage createReceivedMessage(
      String ackId, ByteString data, Map<String, String> attributes) {
    PubsubMessage message =
        PubsubMessage.newBuilder().setData(data).putAllAttributes(attributes).build();
    return ReceivedMessage.newBuilder().setAckId(ackId).setMessage(message).build();
  }
}

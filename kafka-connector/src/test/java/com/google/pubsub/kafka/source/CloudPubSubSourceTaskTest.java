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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.SettableApiFuture;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link CloudPubSubSourceTask}. */
public class CloudPubSubSourceTaskTest {
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSourceTaskTest.class);

  private static final String CPS_PROJECT = "the";
  private static final String CPS_MAX_BATCH_SIZE = "1000";
  private static final String CPS_SUBSCRIPTION = "quick";
  private static final String KAFKA_TOPIC = "brown";
  private static final String KAFKA_MESSAGE_KEY_ATTRIBUTE = "fox";
  private static final String KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE = "jumped";
  private static final String KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE = "over";
  private static final String KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE_VALUE = "1234567890";
  private static final String KAFKA_PARTITIONS = "3";
  private static final ByteString CPS_MESSAGE = ByteString.copyFromUtf8("the");
  private static final byte[] KAFKA_VALUE = CPS_MESSAGE.toByteArray();
  private static final String ACK_ID1 = "ackID1";
  private static final String ACK_ID2 = "ackID2";
  private static final String ACK_ID3 = "ackID3";
  private static final String ACK_ID4 = "ackID4";

  private CloudPubSubSourceTask task;
  private Map<String, String> props;
  private CloudPubSubSubscriber subscriber;

  /**
   * Compare two SourceRecords. This is necessary because the records' values contain a byte[] and
   * the .equals on a SourceRecord does not take this into account.
   */
  public void assertRecordsEqual(SourceRecord sr1, SourceRecord sr2) {
    assertEquals(sr1.key(), sr2.key());
    assertEquals(sr1.keySchema(), sr2.keySchema());
    assertEquals(sr1.valueSchema(), sr2.valueSchema());
    assertEquals(sr1.topic(), sr2.topic());

    if (sr1.valueSchema() == Schema.BYTES_SCHEMA) {
      assertArrayEquals((byte[])sr1.value(), (byte[])sr2.value());
    } else {
      for(Field f : sr1.valueSchema().fields()) {
        if (f.name().equals(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD)) {
          assertArrayEquals(((Struct)sr1.value()).getBytes(f.name()),
                            ((Struct)sr2.value()).getBytes(f.name()));
        } else {
          assertEquals(((Struct)sr1.value()).getString(f.name()),
                       ((Struct)sr2.value()).getString(f.name()));
        }
      }
    }
  }

  @Before
  public void setup() {
    subscriber = mock(CloudPubSubSubscriber.class, RETURNS_DEEP_STUBS);
    task = new CloudPubSubSourceTask(subscriber);
    props = new HashMap<>();
    props.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    props.put(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG, CPS_MAX_BATCH_SIZE);
    props.put(CloudPubSubSourceConnector.CPS_SUBSCRIPTION_CONFIG, CPS_SUBSCRIPTION);
    props.put(CloudPubSubSourceConnector.KAFKA_TOPIC_CONFIG, KAFKA_TOPIC);
    props.put(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG, KAFKA_MESSAGE_KEY_ATTRIBUTE);
    props.put(CloudPubSubSourceConnector.KAFKA_MESSAGE_TIMESTAMP_CONFIG, KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE);
    props.put(CloudPubSubSourceConnector.KAFKA_PARTITIONS_CONFIG, KAFKA_PARTITIONS);
    props.put(
        CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        CloudPubSubSourceConnector.PartitionScheme.ROUND_ROBIN.toString());
  }

  /** Tests when no messages are received from the Cloud Pub/Sub PullResponse. */
  @Test
  public void testPollCaseWithNoMessages() throws Exception {
    task.start(props);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of());
    assertEquals(0, task.poll().size());
    verify(subscriber, never()).ackMessages(any());
  }

  /**
   * Tests that when ackMessages() succeeds and the subsequent call to poll() has no messages, that
   * the subscriber does not invoke ackMessages because there should be no acks.
   */
  @Test
  public void testPollInRegularCase() throws Exception {
    task.start(props);
    ReceivedMessage rm1 =
        createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>(), null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm1));
    List<SourceRecord> result = task.poll();
    assertEquals(1, result.size());
    task.commitRecord(result.get(0));
    SettableApiFuture<Empty> goodFuture = SettableApiFuture.create();
    goodFuture.set(Empty.getDefaultInstance());
    when(subscriber.ackMessages(any())).thenReturn(goodFuture);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of());
    result = task.poll();
    assertEquals(0, result.size());
    result = task.poll();
    assertEquals(0, result.size());
    verify(subscriber, times(1)).ackMessages(any());
  }


  /**
   * Tests that when a call to ackMessages() fails, that the message is not redelivered to Kafka if
   * the message is received again by Cloud Pub/Sub. Also tests that ack ids are added properly if
   * the ack id has not been seen before.
   */
  @Test
  public void testPollWithDuplicateReceivedMessages() throws Exception {
    task.start(props);
    ReceivedMessage rm1 =
        createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>(), null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm1));
    List<SourceRecord> result = task.poll();
    assertEquals(1, result.size());
    ReceivedMessage rm2 =
        createReceivedMessage(ACK_ID2, CPS_MESSAGE, new HashMap<>(), null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm1, rm2));
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
    ReceivedMessage rm =
        createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>(), null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertRecordsEqual(expected, result.get(0));
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
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, attributes, null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertRecordsEqual(expected, result.get(0));
  }

  /**
   * Tests when the message(s) retrieved from Cloud Pub/Sub do have an attribute that matches {@link
   * #KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE} and {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE}.
   */
  @Test
  public void testPollWithMessageTimestampAttribute() throws Exception{
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    attributes.put(KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE, KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE_VALUE);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, attributes, null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(1, result.size());
    SourceRecord expected =
            new SourceRecord(
                    null,
                    null,
                    KAFKA_TOPIC,
                    0,
                    Schema.OPTIONAL_STRING_SCHEMA,
                    KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
                    Schema.BYTES_SCHEMA,
                    KAFKA_VALUE, Long.parseLong(KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE_VALUE));
    assertRecordsEqual(expected, result.get(0));
  }

  /**
   * Tests when the message retrieved from Cloud Pub/Sub have several attributes, including
   * one that matches {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE} and uses Kafka Record Headers to store them
   */
  @Test
  public void testPollWithMultipleAttributesAndRecordHeaders() throws Exception {
    props.put(CloudPubSubSourceConnector.USE_KAFKA_HEADERS, "true");
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    attributes.put("attribute1", "attribute_value1");
    attributes.put("attribute2", "attribute_value2");
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, attributes, null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(1, result.size());

    ConnectHeaders headers = new ConnectHeaders();
    headers.addString("attribute1", "attribute_value1");
    headers.addString("attribute2", "attribute_value2");

    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE,
            Long.parseLong(KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE_VALUE),
            headers);
    assertRecordsEqual(expected, result.get(0));
  }

  /**
   * Tests when the message has an ordering key that should be stored as an attribute.
   */
  @Test
  public void testPollWithOrderingKeyAsAttribute() throws Exception {
    props.put(CloudPubSubSourceConnector.CPS_MAKE_ORDERING_KEY_ATTRIBUTE, "true");
    task.start(props);
    ReceivedMessage rm =
        createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>(), "my-key");
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(1, result.size());

    Schema expectedSchema =
        SchemaBuilder.struct()
            .field(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD, Schema.BYTES_SCHEMA)
            .field(ConnectorUtils.CPS_ORDERING_KEY_ATTRIBUTE, Schema.STRING_SCHEMA)
            .build();
    Struct expectedValue = new Struct(expectedSchema)
                               .put(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD, KAFKA_VALUE)
                               .put(ConnectorUtils.CPS_ORDERING_KEY_ATTRIBUTE, "my-key");

    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            expectedSchema,
            expectedValue);
    assertRecordsEqual(expected, result.get(0));
  }

  /**
   * Tests when the message has an ordering key that should be stored as an attribute in the Kafka
   * Record Headers.
   */
  @Test
  public void testPollWithOrderingKeyAsRecordHeader() throws Exception {
    props.put(CloudPubSubSourceConnector.USE_KAFKA_HEADERS, "true");
    props.put(CloudPubSubSourceConnector.CPS_MAKE_ORDERING_KEY_ATTRIBUTE, "true");
    task.start(props);
    ReceivedMessage rm =
        createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>(), "my-key");
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(1, result.size());

    ConnectHeaders headers = new ConnectHeaders();
    headers.addString(ConnectorUtils.CPS_ORDERING_KEY_ATTRIBUTE, "my-key3");

    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE,
            Long.parseLong(KAFKA_MESSAGE_TIMESTAMP_ATTRIBUTE_VALUE),
            headers);
    assertRecordsEqual(expected, result.get(0));
  }

  /**
   * Tests when the message retrieved from Cloud Pub/Sub have several attributes, including
   * one that matches {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE}
   */
  @Test
  public void testPollWithMultipleAttributes() throws Exception {
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    attributes.put(KAFKA_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE);
    attributes.put("attribute1", "attribute_value1");
    attributes.put("attribute2", "attribute_value2");
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, attributes, null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(1, result.size());
    Schema expectedSchema =
        SchemaBuilder.struct()
            .field(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD, Schema.BYTES_SCHEMA)
            .field("attribute1", Schema.STRING_SCHEMA)
            .field("attribute2", Schema.STRING_SCHEMA)
            .build();
    Struct expectedValue = new Struct(expectedSchema)
                               .put(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD, KAFKA_VALUE)
                               .put("attribute1", "attribute_value1")
                               .put("attribute2", "attribute_value2");
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            expectedSchema,
            expectedValue);
    assertRecordsEqual(expected, result.get(0));
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
    ReceivedMessage withoutKey =
        createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>(), null);
    ReceivedMessage withKey = createReceivedMessage(ACK_ID2, CPS_MESSAGE, attributes, null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(withKey, withoutKey));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(2, result.size());
    SourceRecord expectedForMessageWithKey =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE.hashCode() % Integer.parseInt(KAFKA_PARTITIONS),
            Schema.OPTIONAL_STRING_SCHEMA,
            KAFKA_MESSAGE_KEY_ATTRIBUTE_VALUE,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expectedForMessageWithoutKey =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);

    assertRecordsEqual(expectedForMessageWithKey, result.get(0));
    assertArrayEquals((byte[])expectedForMessageWithoutKey.value(), (byte[])result.get(1).value());
  }

  /** Tests that the correct partition is assigned when the partition scheme is "hash_value". */
  @Test
  public void testPollWithPartitionSchemeHashValue() throws Exception {
    props.put(
        CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        CloudPubSubSourceConnector.PartitionScheme.HASH_VALUE.toString());
    task.start(props);
    ReceivedMessage rm =
        createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>(), null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            KAFKA_VALUE.hashCode() % Integer.parseInt(KAFKA_PARTITIONS),
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertRecordsEqual(expected, result.get(0));
  }

  /** Tests that the no partition is assigned when the partition scheme is "kafka_partitioner". */
  @Test
  public void testPollWithPartitionSchemeKafkaPartitioner() throws Exception {
    props.put(
            CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
            CloudPubSubSourceConnector.PartitionScheme.KAFKA_PARTITIONER.toString());
    task.start(props);
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>(), null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(1, result.size());
    SourceRecord expected =
            new SourceRecord(
                    null,
                    null,
                    KAFKA_TOPIC,
                    null,
                    Schema.OPTIONAL_STRING_SCHEMA,
                    null,
                    Schema.BYTES_SCHEMA,
                    KAFKA_VALUE);
    assertRecordsEqual(expected, result.get(0));
    assertNull(result.get(0).kafkaPartition());
  }

  /** Tests that the correct partition is assigned when the partition scheme is "ordering_key". */
  @Test
  public void testPollWithPartitionSchemaOrderingKey() throws Exception {
    String orderingKey = "my-key";
    props.put(
        CloudPubSubSourceConnector.KAFKA_PARTITION_SCHEME_CONFIG,
        CloudPubSubSourceConnector.PartitionScheme.ORDERING_KEY.toString());
    task.start(props);
    ReceivedMessage rm =
        createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>(), orderingKey);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(1, result.size());
    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            orderingKey.hashCode() % Integer.parseInt(KAFKA_PARTITIONS),
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertRecordsEqual(expected, result.get(0));
  }

  /**
   * Tests that the correct partition is assigned when the partition scheme is "round_robin". The
   * tests makes sure to submit an approrpriate number of messages to poll() so that all partitions
   * in the round robin are hit once.
   */
  @Test
  public void testPollWithPartitionSchemeRoundRobin() throws Exception {
    task.start(props);
    ReceivedMessage rm1 =
        createReceivedMessage(ACK_ID1, CPS_MESSAGE, new HashMap<>(), null);
    ReceivedMessage rm2 =
        createReceivedMessage(ACK_ID2, CPS_MESSAGE, new HashMap<>(), null);
    ReceivedMessage rm3 =
        createReceivedMessage(ACK_ID3, CPS_MESSAGE, new HashMap<>(), null);
    ReceivedMessage rm4 =
        createReceivedMessage(ACK_ID4, CPS_MESSAGE, new HashMap<>(), null);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm1, rm2, rm3, rm4));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(4, result.size());
    SourceRecord expected1 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expected2 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            1,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expected3 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            2,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    SourceRecord expected4 =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertRecordsEqual(expected1, result.get(0));
    assertRecordsEqual(expected2, result.get(1));
    assertRecordsEqual(expected3, result.get(2));
    assertRecordsEqual(expected4, result.get(3));
  }

  /**
   * Tests when the message retrieved from Cloud Pub/Sub has an ordering key set and
   * {@link #KAFKA_MESSAGE_KEY_ATTRIBUTE} is set to "orderingKey".
   */
  @Test
  public void testSetOrderingKeyAsKey() throws Exception {
    String orderingKey = "my-key";
    props.put(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG, ConnectorUtils.CPS_ORDERING_KEY_ATTRIBUTE);
    task.start(props);
    Map<String, String> attributes = new HashMap<>();
    ReceivedMessage rm = createReceivedMessage(ACK_ID1, CPS_MESSAGE, attributes, orderingKey);
    when(subscriber.pull().get()).thenReturn(ImmutableList.of(rm));
    List<SourceRecord> result = task.poll();
    verify(subscriber, never()).ackMessages(any());
    assertEquals(1, result.size());

    SourceRecord expected =
        new SourceRecord(
            null,
            null,
            KAFKA_TOPIC,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            orderingKey,
            Schema.BYTES_SCHEMA,
            KAFKA_VALUE);
    assertRecordsEqual(expected, result.get(0));
  }

  @Test
  public void testPollExceptionCase() throws Exception {
    task.start(props);
    // Could also throw ExecutionException if we wanted to...
    when(subscriber.pull().get()).thenThrow(new InterruptedException());
    assertEquals(0, task.poll().size());
  }

  private ReceivedMessage createReceivedMessage(
      String ackId, ByteString data, Map<String, String> attributes, String orderingKey) {
    PubsubMessage.Builder builder = PubsubMessage.newBuilder().setData(data).putAllAttributes(attributes);
    if (orderingKey != null) {
      builder.setOrderingKey(orderingKey);
    }
    return ReceivedMessage.newBuilder().setAckId(ackId).setMessage(builder.build()).build();
  }
}

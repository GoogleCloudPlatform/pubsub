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
package com.google.pubsub.kafka.sink;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Tests for {@link CloudPubSubSinkTask}. */
public class CloudPubSubSinkTaskTest {

  private static final String CPS_TOPIC = "the";
  private static final String CPS_PROJECT = "quick";
  private static final String CPS_MIN_BATCH_SIZE1 = "2";
  private static final String CPS_MIN_BATCH_SIZE2 = "9";
  private static final String KAFKA_TOPIC = "brown";
  private static final ByteString KAFKA_MESSAGE1 = ByteString.copyFromUtf8("fox");
  private static final ByteString KAFKA_MESSAGE2 = ByteString.copyFromUtf8("jumped");
  private static final String FIELD_STRING1 = "Roll";
  private static final String FIELD_STRING2 = "War";
  private static final String KAFKA_MESSAGE_KEY = "over";
  private static final Schema STRING_SCHEMA = SchemaBuilder.string().build();
  private static final Schema BYTE_STRING_SCHEMA =
      SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build();

  private CloudPubSubSinkTask task;
  private Map<String, String> props;
  private CloudPubSubPublisher publisher;

  @Before
  public void setup() {
    publisher = mock(CloudPubSubPublisher.class, RETURNS_DEEP_STUBS);
    task = new CloudPubSubSinkTask(publisher);
    props = new HashMap<>();
    props.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
    props.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE2);
  }

  /** Tests that an exception is thrown when the schema of the value is not BYTES. */
  @Test
  public void testPutPrimitives() {
    task.start(props);
    SinkRecord record8 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int8(), (byte) 5, -1);
    SinkRecord record16 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int16(), (short) 5, -1);
    SinkRecord record32 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int32(), (int) 5, -1);
    SinkRecord record64 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.int64(), (long) 5, -1);
    SinkRecord recordFloat32 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.float32(), (float) 8, -1);
    SinkRecord recordFloat64 =
        new SinkRecord(null, -1, null, null, SchemaBuilder.float64(), (double) 8, -1);
    SinkRecord recordBool =
        new SinkRecord(null, -1, null, null, SchemaBuilder.bool(), true, -1);
    SinkRecord recordString =
        new SinkRecord(null, -1, null, null, SchemaBuilder.string(), "Test put.", -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record8);
    list.add(record16);
    list.add(record32);
    list.add(record64);
    list.add(recordFloat32);
    list.add(recordFloat64);
    list.add(recordBool);
    list.add(recordString);
    task.put(list);
  }

  @Test
  public void testStructSchema() {
    task.start(props);
    Schema schema = SchemaBuilder.struct().field(FIELD_STRING1, SchemaBuilder.string())
        .field(FIELD_STRING2, SchemaBuilder.string()).build();
    Struct val = new Struct(schema);
    val.put(FIELD_STRING1, "tide");
    val.put(FIELD_STRING2, "eagle");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
    schema = SchemaBuilder.struct().field(FIELD_STRING1, SchemaBuilder.struct()).build();
    record = new SinkRecord(null, -1, null, null, schema, new Struct(schema), -1);
    list.add(record);
    try {
      task.put(list);
    } catch (DataException e) { } // Expected, pass.
  }

  @Test
  public void testMapSchema() {
    task.start(props);
    Schema schema = SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).build();
    Map<String, String> val = new HashMap<>();
    val.put(FIELD_STRING1, "tide");
    val.put(FIELD_STRING2, "eagle");
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
    schema = SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.bytes()).build();
    record = new SinkRecord(null, -1, null, null, schema, val, -1);
    list.add(record);
    try {
      task.put(list);
    } catch (DataException e) { } // Expected, pass.
  }

  @Test
  public void testArraySchema() {
    task.start(props);
    Schema schema = SchemaBuilder.array(SchemaBuilder.string()).build();
    String[] val = {"Roll", "tide"};
    SinkRecord record = new SinkRecord(null, -1, null, null, schema, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
    schema = SchemaBuilder.array(SchemaBuilder.struct()).build();
    record = new SinkRecord(null, -1, null, null, schema, null, -1);
    list.add(record);
    try {
      task.put(list);
    } catch (DataException e) { } // Expected, pass.
  }

  @Test
  public void testNullSchema() {
    task.start(props);
    String val = "I have no schema";
    SinkRecord record = new SinkRecord(null, -1, null, null, null, val, -1);
    List<SinkRecord> list = new ArrayList<>();
    list.add(record);
    task.put(list);
  }

  /**
   * Tests that if there are not enough messages buffered, publisher.publish() is not invoked.
   */
  @Test
  public void testPutWhereNoPublishesAreInvoked() {
    task.start(props);
    List<SinkRecord> records = getSampleRecords();
    task.put(records);
    verify(publisher, never()).publish(any(PublishRequest.class));
  }

  /**
   * Tests that if there are enough messages buffered, that the PublishRequest sent to the publisher
   * is correct.
   */
  @Test
  public void testPutWherePublishesAreInvoked() {
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = getSampleRecords();
    task.put(records);
    ArgumentCaptor<PublishRequest> captor = ArgumentCaptor.forClass(PublishRequest.class);
    verify(publisher, times(1)).publish(captor.capture());
    PublishRequest requestArg = captor.getValue();
    assertEquals(requestArg.getMessagesList(), getPubsubMessagesFromSampleRecords());
  }

  /**
   * Tests that a call to flush() processes the Futures that were generated during this same call
   * to flush() (i.e buffered messages were not published until the call to flush()).
   */
  @Test
  public void testFlushWithNoPublishInPut() throws Exception {
    task.start(props);
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, 0), null);
    List<SinkRecord> records = getSampleRecords();
    ListenableFuture<PublishResponse> goodFuture =
        spy(Futures.immediateFuture(PublishResponse.getDefaultInstance()));
    when(publisher.publish(any(PublishRequest.class))).thenReturn(goodFuture);
    task.put(records);
    task.flush(partitionOffsets);
    verify(publisher, times(1)).publish(any(PublishRequest.class));
    verify(goodFuture, times(1)).get();
  }

  /**
   * Tests that a call to flush() processes the Futures that were generated during a previous
   * call to put() (i.e enough messages were buffered in put() to trigger a publish).
   */
  @Test
  public void testFlushWithPublishInPut() throws Exception {
    props.put(CloudPubSubSinkConnector.MAX_BUFFER_SIZE_CONFIG, CPS_MIN_BATCH_SIZE1);
    task.start(props);
    List<SinkRecord> records = getSampleRecords();
    ListenableFuture<PublishResponse> goodFuture =
        spy(Futures.immediateFuture(PublishResponse.getDefaultInstance()));
    when(publisher.publish(any(PublishRequest.class))).thenReturn(goodFuture);
    task.put(records);
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, 0), null);
    task.flush(partitionOffsets);
    verify(goodFuture, times(1)).get();
    ArgumentCaptor<PublishRequest> captor = ArgumentCaptor.forClass(PublishRequest.class);
    verify(publisher, times(1)).publish(captor.capture());
    PublishRequest requestArg = captor.getValue();
    assertEquals(requestArg.getMessagesList(), getPubsubMessagesFromSampleRecords());
  }

  /**
   * Tests that if a Future that is being processed in flush() failed with an exception, that an
   * exception is thrown.
   */
  @Test(expected = RuntimeException.class)
  public void testFlushExceptionCase() throws Exception {
    task.start(props);
    Map<TopicPartition, OffsetAndMetadata> partitionOffsets = new HashMap<>();
    partitionOffsets.put(new TopicPartition(KAFKA_TOPIC, 0), null);
    List<SinkRecord> records = getSampleRecords();
    ListenableFuture<PublishResponse> badFuture = spy(Futures.<PublishResponse>immediateFailedFuture(new Exception()));
    when(publisher.publish(any(PublishRequest.class))).thenReturn(badFuture);
    task.put(records);
    task.flush(partitionOffsets);
    verify(publisher, times(1)).publish(any(PublishRequest.class));
    verify(badFuture, times(1)).get();
  }

  /** Get some sample SinkRecords's to use in the tests. */
  private List<SinkRecord> getSampleRecords() {
    List<SinkRecord> records = new ArrayList<>();
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE1,
            -1));
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            STRING_SCHEMA,
            KAFKA_MESSAGE_KEY,
            BYTE_STRING_SCHEMA,
            KAFKA_MESSAGE2,
            -1));
    return records;
  }

  /**
   * Get some PubsubMessage's which correspond to the SinkRecord's created in {@link
   * #getSampleRecords()}.
   */
  private List<PubsubMessage> getPubsubMessagesFromSampleRecords() {
    List<PubsubMessage> messages = new ArrayList<>();
    Map<String, String> attributes = new HashMap<>();
    attributes.put(ConnectorUtils.CPS_MESSAGE_KEY_ATTRIBUTE, KAFKA_MESSAGE_KEY);
    messages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes).setData(KAFKA_MESSAGE1).build());
    messages.add(
        PubsubMessage.newBuilder().putAllAttributes(attributes).setData(KAFKA_MESSAGE2).build());
    return messages;
  }
}

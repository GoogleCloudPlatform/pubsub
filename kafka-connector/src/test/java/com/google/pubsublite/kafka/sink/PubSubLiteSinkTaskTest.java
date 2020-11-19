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
package com.google.pubsublite.kafka.sink;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.PublishMetadata;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Spy;

@RunWith(JUnit4.class)
public class PubSubLiteSinkTaskTest {

  private static final String KAFKA_MESSAGE_KEY1 = "dog";
  private static final String KAFKA_MESSAGE_KEY2 = "cat";
  private static final String KAFKA_TOPIC = "brown";
  private static final ByteString KAFKA_MESSAGE1 = ByteString.copyFromUtf8("fox");
  private static final ByteString KAFKA_MESSAGE2 = ByteString.copyFromUtf8("jumps");

  private static final SinkRecord SAMPLE_RECORD_1 = new SinkRecord(
      KAFKA_TOPIC,
      0,
      Schema.STRING_SCHEMA,
      KAFKA_MESSAGE_KEY1,
      Schema.BYTES_SCHEMA,
      KAFKA_MESSAGE1.toByteArray(),
      -1);
  private static final SinkRecord SAMPLE_RECORD_2 = new SinkRecord(
      KAFKA_TOPIC,
      0,
      Schema.STRING_SCHEMA,
      KAFKA_MESSAGE_KEY2,
      Schema.BYTES_SCHEMA,
      KAFKA_MESSAGE2.toByteArray(),
      -1);
  private static final Message SAMPLE_MESSAGE_1 = Message.builder()
      .setKey(ByteString.copyFromUtf8(KAFKA_MESSAGE_KEY1))
      .setData(KAFKA_MESSAGE1)
      .setAttributes(ImmutableListMultimap.<String, ByteString>builder()
          .put(Constants.KAFKA_TOPIC_HEADER, ByteString.copyFromUtf8(KAFKA_TOPIC))
          .put(Constants.KAFKA_PARTITION_HEADER, ByteString.copyFromUtf8(Integer.toString(0)))
          .put(Constants.KAFKA_OFFSET_HEADER, ByteString.copyFromUtf8(Integer.toString(-1)))
          .build()).build();
  private static final Message SAMPLE_MESSAGE_2 = Message.builder()
      .setKey(ByteString.copyFromUtf8(KAFKA_MESSAGE_KEY2))
      .setData(KAFKA_MESSAGE2)
      .setAttributes(ImmutableListMultimap.<String, ByteString>builder()
          .put(Constants.KAFKA_TOPIC_HEADER, ByteString.copyFromUtf8(KAFKA_TOPIC))
          .put(Constants.KAFKA_PARTITION_HEADER, ByteString.copyFromUtf8(Integer.toString(0)))
          .put(Constants.KAFKA_OFFSET_HEADER, ByteString.copyFromUtf8(Integer.toString(-1)))
          .build()).build();

  private PubSubLiteSinkTask task;

  abstract static class FakePublisher extends FakeApiService
      implements Publisher<PublishMetadata> {

  }

  private @Spy
  FakePublisher publisher;

  @Before
  public void setup() {
    initMocks(this);
    assertNotNull(publisher);
    task = new PubSubLiteSinkTask(map -> publisher);
    task.start(ImmutableMap.of());
  }

  @After
  public void tearDown() {
    if (task != null) {
      task.stop();
    }
  }

  /**
   * Tests handling of primitives.
   */
  @Test
  public void testPutPrimitives() {
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
    verify(publisher, times(8)).publish(any());
  }

  /**
   * Tests that the correct message is sent to the publisher.
   */
  @Test
  public void testPutWherePublishesAreInvoked() {
    InOrder order = inOrder(publisher);
    task.put(ImmutableList.of(SAMPLE_RECORD_1, SAMPLE_RECORD_2));
    order.verify(publisher).publish(SAMPLE_MESSAGE_1);
    order.verify(publisher).publish(SAMPLE_MESSAGE_2);
  }

  /**
   * Tests that the correct message is sent to the publisher when the record has a null value.
   */
  @Test
  public void testPutWithNullValues() {
    List<SinkRecord> records = new ArrayList<>();
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            Schema.STRING_SCHEMA,
            KAFKA_MESSAGE_KEY1,
            Schema.BYTES_SCHEMA,
            null,
            -1));
    task.put(records);
    Message expectedResult = SAMPLE_MESSAGE_1.toBuilder().setData(ByteString.EMPTY).build();
    verify(publisher).publish(expectedResult);
  }

  /**
   * Tests that the message can be completely null.
   */
  @Test
  public void testPutWithNullMessage() {
    List<SinkRecord> records = new ArrayList<>();
    records.add(
        new SinkRecord(
            KAFKA_TOPIC,
            0,
            Schema.STRING_SCHEMA,
            null,
            Schema.BYTES_SCHEMA,
            null,
            -1));
    task.put(records);
    Message expectedResult = SAMPLE_MESSAGE_1.toBuilder().setKey(ByteString.EMPTY)
        .setData(ByteString.EMPTY).build();
    verify(publisher).publish(expectedResult);
  }

  /**
   * Tests that a call to flush() calls publisher.flush().
   */
  @Test
  public void testFlush() throws Exception {
    task.put(ImmutableList.of(SAMPLE_RECORD_1, SAMPLE_RECORD_2));
    task.flush(ImmutableMap.of());
    InOrder order = inOrder(publisher);
    order.verify(publisher).publish(SAMPLE_MESSAGE_1);
    order.verify(publisher).publish(SAMPLE_MESSAGE_2);
    order.verify(publisher).flush();
  }

  /**
   * Tests that a call to flush() before start() is fine.
   */
  @Test
  public void testFlushBeforeStart() {
    task = new PubSubLiteSinkTask();
    task.flush(ImmutableMap.of());
    task = null;
  }

  /**
   * Tests that if flush() throws an exception, an exception is thrown.
   */
  @Test(expected = RuntimeException.class)
  public void testFlushExceptionCase() throws Exception {
    doThrow(new IOException("bad flush")).when(publisher).flush();
    task.put(ImmutableList.of(SAMPLE_RECORD_1));
    verify(publisher).publish(SAMPLE_MESSAGE_1);
    try {
      task.flush(ImmutableMap.of());
    } finally {
      task = null;
    }
  }

  /**
   * Tests that Kafka metadata is included in the messages published to Pub/Sub Lite.
   */
  @Test
  public void testKafkaMetadata() {
    SinkRecord record1 = new SinkRecord(
        KAFKA_TOPIC,
        4,
        Schema.STRING_SCHEMA,
        KAFKA_MESSAGE_KEY1,
        Schema.BYTES_SCHEMA,
        KAFKA_MESSAGE1.toByteArray(),
        1000,
        50000L,
        TimestampType.CREATE_TIME);
    SinkRecord record2 = new SinkRecord(
        KAFKA_TOPIC,
        4,
        Schema.STRING_SCHEMA,
        KAFKA_MESSAGE_KEY1,
        Schema.BYTES_SCHEMA,
        KAFKA_MESSAGE1.asReadOnlyByteBuffer(),
        1001,
        50001L,
        TimestampType.LOG_APPEND_TIME);
    SinkRecord record3 = new SinkRecord(
        KAFKA_TOPIC,
        4,
        Schema.STRING_SCHEMA,
        KAFKA_MESSAGE_KEY1,
        Schema.BYTES_SCHEMA,
        KAFKA_MESSAGE1.toByteArray(),
        1002,
        null,
        TimestampType.CREATE_TIME);
    task.put(ImmutableList.of(record1, record2, record3));
    ImmutableListMultimap<String, ByteString> attributesBase = ImmutableListMultimap.<String, ByteString>builder()
        .put(Constants.KAFKA_TOPIC_HEADER, ByteString.copyFromUtf8(KAFKA_TOPIC))
        .put(Constants.KAFKA_PARTITION_HEADER, ByteString.copyFromUtf8(Integer.toString(4)))
        .build();
    Message expectedBase = Message.builder()
        .setKey(ByteString.copyFromUtf8(KAFKA_MESSAGE_KEY1))
        .setData(KAFKA_MESSAGE1).build();
    Message message1 = expectedBase.toBuilder()
        .setEventTime(Timestamps.fromMillis(50000))
        .setAttributes(ImmutableListMultimap.<String, ByteString>builder().putAll(attributesBase)
            .put(Constants.KAFKA_OFFSET_HEADER, ByteString.copyFromUtf8("1000"))
            .put(Constants.KAFKA_EVENT_TIME_TYPE_HEADER, ByteString.copyFromUtf8("CreateTime"))
            .build()).build();
    Message message2 = expectedBase.toBuilder()
        .setEventTime(Timestamps.fromMillis(50001))
        .setAttributes(ImmutableListMultimap.<String, ByteString>builder().putAll(attributesBase)
            .put(Constants.KAFKA_OFFSET_HEADER, ByteString.copyFromUtf8("1001"))
            .put(Constants.KAFKA_EVENT_TIME_TYPE_HEADER, ByteString.copyFromUtf8("LogAppendTime"))
            .build()).build();
    Message message3 = expectedBase.toBuilder()
        .setAttributes(ImmutableListMultimap.<String, ByteString>builder().putAll(attributesBase)
            .put(Constants.KAFKA_OFFSET_HEADER, ByteString.copyFromUtf8("1002"))
            .build()).build();
    InOrder order = inOrder(publisher);
    order.verify(publisher).publish(message1);
    order.verify(publisher).publish(message2);
    order.verify(publisher).publish(message3);
  }

  /**
   * Tests that Kafka headers are included in the messages published to Pub/Sub Lite.
   */
  @Test
  public void testKafkaHeaders() {
    SinkRecord record1 = new SinkRecord(
        KAFKA_TOPIC,
        4,
        Schema.STRING_SCHEMA,
        KAFKA_MESSAGE_KEY1,
        Schema.BYTES_SCHEMA,
        KAFKA_MESSAGE1.toByteArray(),
        1000,
        50000L,
        TimestampType.CREATE_TIME);
    record1.headers().addString("myHeader", "myValue");
    SinkRecord record2 = new SinkRecord(
        KAFKA_TOPIC,
        4,
        Schema.STRING_SCHEMA,
        KAFKA_MESSAGE_KEY1,
        Schema.BYTES_SCHEMA,
        KAFKA_MESSAGE1.asReadOnlyByteBuffer(),
        1001,
        50001L,
        TimestampType.LOG_APPEND_TIME);
    record2.headers().addString("yourHeader", "yourValue");
    task.put(ImmutableList.of(record1, record2));
    ImmutableListMultimap<String, ByteString> attributesBase = ImmutableListMultimap.<String, ByteString>builder()
        .put(Constants.KAFKA_TOPIC_HEADER, ByteString.copyFromUtf8(KAFKA_TOPIC))
        .put(Constants.KAFKA_PARTITION_HEADER, ByteString.copyFromUtf8(Integer.toString(4)))
        .build();
    Message expectedBase = Message.builder()
        .setKey(ByteString.copyFromUtf8(KAFKA_MESSAGE_KEY1))
        .setData(KAFKA_MESSAGE1).build();
    Message message1 = expectedBase.toBuilder()
        .setEventTime(Timestamps.fromMillis(50000))
        .setAttributes(ImmutableListMultimap.<String, ByteString>builder().putAll(attributesBase)
            .put(Constants.KAFKA_OFFSET_HEADER, ByteString.copyFromUtf8("1000"))
            .put(Constants.KAFKA_EVENT_TIME_TYPE_HEADER, ByteString.copyFromUtf8("CreateTime"))
            .put("myHeader", ByteString.copyFromUtf8("myValue"))
            .build()).build();
    Message message2 = expectedBase.toBuilder()
        .setEventTime(Timestamps.fromMillis(50001))
        .setAttributes(ImmutableListMultimap.<String, ByteString>builder().putAll(attributesBase)
            .put(Constants.KAFKA_OFFSET_HEADER, ByteString.copyFromUtf8("1001"))
            .put(Constants.KAFKA_EVENT_TIME_TYPE_HEADER, ByteString.copyFromUtf8("LogAppendTime"))
            .put("yourHeader", ByteString.copyFromUtf8("yourValue"))
            .build()).build();
    InOrder order = inOrder(publisher);
    order.verify(publisher).publish(message1);
    order.verify(publisher).publish(message2);
  }
}

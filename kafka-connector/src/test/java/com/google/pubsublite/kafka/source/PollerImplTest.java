package com.google.pubsublite.kafka.source;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.TopicPath;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;

public class PollerImplTest {

  private static final String KAFKA_TOPIC = "kafka-topic";

  @Mock
  Consumer<byte[], byte[]> underlying;
  @Mock
  Headers mockHeaders;
  Poller poller;

  @Before
  public void setUp() {
    initMocks(this);
    poller = new PollerImpl(KAFKA_TOPIC, underlying);
  }

  @Test
  public void pollTimeout() {
    when(underlying.poll(PollerImpl.POLL_DURATION)).thenThrow(new TimeoutException(""));
    assertThat(poller.poll()).isNull();
  }

  @Test
  public void pollWakeup() {
    when(underlying.poll(PollerImpl.POLL_DURATION)).thenThrow(new WakeupException());
    assertThat(poller.poll()).isNull();
  }

  private static Header toHeader(String key, ByteString value) {
    return new Header() {
      @Override
      public String key() {
        return key;
      }

      @Override
      public byte[] value() {
        return value.toByteArray();
      }
    };
  }

  @Test
  public void pollTranslates() {
    ByteString key = ByteString.copyFromUtf8("key");
    ByteString value = ByteString.copyFromUtf8("value");
    ImmutableListMultimap<String, ByteString> sourceHeadersMap = ImmutableListMultimap.of(
        "one", ByteString.copyFromUtf8("a"),
        "two", ByteString.copyFromUtf8("b"),
        "two", ByteString.copyFromUtf8("c")
    );
    when(mockHeaders.iterator()).thenReturn(Iterators.transform(
        sourceHeadersMap.entries().iterator(),
        entry -> toHeader(entry.getKey(), entry.getValue())));
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        example(TopicPath.class).toString(), (int) example(Partition.class).value(),
        example(Offset.class).value(), 1000L, TimestampType.NO_TIMESTAMP_TYPE, 0L,
        key.size(), value.size(), key.toByteArray(), value.toByteArray(), mockHeaders
    );
    when(underlying.poll(PollerImpl.POLL_DURATION)).thenReturn(new ConsumerRecords<>(ImmutableMap
        .of(new TopicPartition(example(TopicPath.class).toString(),
            (int) example(Partition.class).value()), ImmutableList.of(record))));

    List<SourceRecord> results = poller.poll();
    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(1);
    SourceRecord result = results.get(0);
    assertThat(result.sourcePartition()).isEqualTo(ImmutableMap
        .of("topic", example(TopicPath.class).toString(), "partition",
            (int) example(Partition.class).value()));
    assertThat(result.sourceOffset())
        .isEqualTo(ImmutableMap.of("offset", example(Offset.class).value()));
    assertThat(result.timestamp()).isEqualTo(1000L);
    assertThat(result.keySchema().isOptional()).isTrue();
    assertThat(result.keySchema().type()).isEqualTo(Schema.Type.BYTES);
    assertThat(result.key()).isEqualTo(key.toByteArray());
    assertThat(result.valueSchema().isOptional()).isFalse();
    assertThat(result.valueSchema().type()).isEqualTo(Schema.Type.BYTES);
    assertThat(result.value()).isEqualTo(value.toByteArray());
    ImmutableListMultimap.Builder<String, byte[]> resultHeadersBuilder = ImmutableListMultimap
        .builder();
    result.headers().forEach(header -> {
      resultHeadersBuilder.put(header.key(), (byte[]) header.value());
      assertThat(header.schema().isOptional()).isFalse();
      assertThat(header.schema().type()).isEqualTo(Schema.Type.BYTES);
    });
    ListMultimap<String, byte[]> resultHeaders = resultHeadersBuilder.build();
    assertThat(resultHeaders.get("one").size()).isEqualTo(1);
    assertThat(resultHeaders.get("two").size()).isEqualTo(2);
    assertThat(resultHeaders.get("one").get(0))
        .isEqualTo(sourceHeadersMap.get("one").get(0).toByteArray());
    assertThat(resultHeaders.get("two").get(0))
        .isEqualTo(sourceHeadersMap.get("two").get(0).toByteArray());
    assertThat(resultHeaders.get("two").get(1))
        .isEqualTo(sourceHeadersMap.get("two").get(1).toByteArray());
  }

  @Test
  public void pollTreatsEmptyKeyAsNull() {
    ByteString key = ByteString.copyFromUtf8("");
    ByteString value = ByteString.copyFromUtf8("value");
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        example(TopicPath.class).toString(), (int) example(Partition.class).value(),
        example(Offset.class).value(), 1000L, TimestampType.NO_TIMESTAMP_TYPE, 0L,
        key.size(), value.size(), key.toByteArray(), value.toByteArray());
    when(underlying.poll(PollerImpl.POLL_DURATION)).thenReturn(new ConsumerRecords<>(ImmutableMap
        .of(new TopicPartition(example(TopicPath.class).toString(),
            (int) example(Partition.class).value()), ImmutableList.of(record))));

    List<SourceRecord> results = poller.poll();
    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(1);
    SourceRecord result = results.get(0);
    assertThat(result.key()).isNull();
  }

  @Test
  public void closeCallsWakeup() {
    poller.close();
    InOrder order = inOrder(underlying);
    order.verify(underlying).wakeup();
    order.verify(underlying).unsubscribe();
  }
}

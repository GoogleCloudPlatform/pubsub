package com.google.pubsublite.kafka.sink;

import static com.google.pubsublite.kafka.sink.Schemas.encodeToBytes;

import com.google.api.core.ApiService.State;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.PublishMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableListMultimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import com.google.cloud.pubsublite.internal.Publisher;

public class PubSubLiteSinkTask extends SinkTask {

  private final PublisherFactory factory;
  private @Nullable
  Publisher<PublishMetadata> publisher;

  @VisibleForTesting
  PubSubLiteSinkTask(PublisherFactory factory) {
    this.factory = factory;
  }

  public PubSubLiteSinkTask() {
    this(new PublisherFactoryImpl());
  }

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    if (publisher != null) {
      throw new IllegalStateException("Called start when publisher already exists.");
    }
    publisher = factory.newPublisher(map);
    publisher.startAsync().awaitRunning();
  }

  @Override
  public void put(Collection<SinkRecord> collection) {
    if (publisher.state() != State.RUNNING) {
      if (publisher.state() == State.FAILED) {
        throw new IllegalStateException("Publisher has failed.", publisher.failureCause());
      } else {
        throw new IllegalStateException("Publisher not currently running.");
      }
    }
    for (SinkRecord record : collection) {
      Message.Builder message = Message.builder();
      if (record.key() != null) {
        message.setKey(encodeToBytes(record.keySchema(), record.key()));
      }
      if (record.value() != null) {
        message.setData(encodeToBytes(record.valueSchema(), record.value()));
      }
      ImmutableListMultimap.Builder<String, ByteString> attributes = ImmutableListMultimap
          .builder();
      getRecordHeaders(record).forEach(header -> attributes
          .put(header.key(), Schemas.encodeToBytes(header.schema(), header.value())));
      if (record.topic() != null) {
        attributes.put(Constants.KAFKA_TOPIC_HEADER, ByteString.copyFromUtf8(record.topic()));
      }
      if (record.kafkaPartition() != null) {
        attributes.put(Constants.KAFKA_PARTITION_HEADER,
            ByteString.copyFromUtf8(record.kafkaPartition().toString()));
        attributes.put(Constants.KAFKA_OFFSET_HEADER,
            ByteString.copyFromUtf8(Long.toString(record.kafkaOffset())));
      }
      if (record.timestamp() != null) {
        attributes.put(Constants.KAFKA_EVENT_TIME_TYPE_HEADER,
            ByteString.copyFromUtf8(record.timestampType().name));
        message.setEventTime(Timestamps.fromMillis(record.timestamp()));
      }
      message.setAttributes(attributes.build());
      publisher.publish(message.build());
    }
  }

  private Iterable<? extends Header> getRecordHeaders(SinkRecord record) {
    ConnectHeaders headers = new ConnectHeaders();
    if (record.headers() != null) {
      for (Header header : record.headers()) {
        headers.add(header);
      }
    }
    return headers;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    try {
      if (publisher != null) {
        publisher.flush();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    if (publisher == null) {
      throw new IllegalStateException("Called stop when publisher doesn't exist.");
    }
    try {
      publisher.flush();
      publisher.stopAsync().awaitTerminated();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      publisher = null;
    }
  }
}

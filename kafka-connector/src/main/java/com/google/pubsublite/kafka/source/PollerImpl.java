package com.google.pubsublite.kafka.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

class PollerImpl implements Poller {

  @VisibleForTesting
  static final Duration POLL_DURATION = Duration.ofSeconds(10);
  private final String kafkaTopic;
  private final Consumer<byte[], byte[]> consumer;

  PollerImpl(String kafkaTopic, Consumer<byte[], byte[]> consumer) {
    this.kafkaTopic = kafkaTopic;
    this.consumer = consumer;
  }

  @Override
  public @Nullable
  List<SourceRecord> poll() {
    try {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_DURATION);
      ImmutableList.Builder<SourceRecord> output = ImmutableList.builder();
      records.forEach(consumerRecord -> {
        final ConnectHeaders headers = new ConnectHeaders();
        for (Header header : consumerRecord.headers()) {
          headers.addBytes(header.key(), header.value());
        }
        boolean keyNullOrEmpty = consumerRecord.key() == null || consumerRecord.key().length == 0;
        output.add(new SourceRecord(
            ImmutableMap
                .of("topic", consumerRecord.topic(), "partition", consumerRecord.partition()),
            ImmutableMap.of("offset", consumerRecord.offset()),
            kafkaTopic,
            // Null partition uses the default kafka partitioner which has key affinity if the key
            // is null. Pub/Sub Lite messages with an empty key are treated as if they have no key,
            // and are given a null key instead to get this routing behavior.
            // https://docs.confluent.io/3.3.0/connect/connect-storage-cloud/kafka-connect-s3/docs/configuration_options.html#partitioner
            null,
            Schema.OPTIONAL_BYTES_SCHEMA,
            keyNullOrEmpty ? null : consumerRecord.key(),
            Schema.BYTES_SCHEMA,
            consumerRecord.value(),
            consumerRecord.timestamp(),
            headers
        ));
      });
      return output.build();
    } catch (TimeoutException | WakeupException e) {
      return null;
    }
  }

  @Override
  public void close() {
    consumer.wakeup();
    consumer.unsubscribe();
  }
}

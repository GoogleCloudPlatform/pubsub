package com.google.pubsublite.kafka.sink;

public final class Constants {

  private Constants() {
  }

  public static final String KAFKA_TOPIC_HEADER = "x-goog-pubsublite-source-kafka-topic";
  public static final String KAFKA_PARTITION_HEADER = "x-goog-pubsublite-source-kafka-partition";
  public static final String KAFKA_OFFSET_HEADER = "x-goog-pubsublite-source-kafka-offset";
  public static final String KAFKA_EVENT_TIME_TYPE_HEADER = "x-goog-pubsublite-source-kafka-event-time-type";

}

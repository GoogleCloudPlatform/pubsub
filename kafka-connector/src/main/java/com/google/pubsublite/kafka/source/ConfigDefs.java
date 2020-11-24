package com.google.pubsublite.kafka.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;

final class ConfigDefs {

  private ConfigDefs() {
  }

  static final String PROJECT_FLAG = "pubsublite.project";
  static final String LOCATION_FLAG = "pubsublite.location";
  static final String SUBSCRIPTION_NAME_FLAG = "pubsublite.subscription";
  static final String KAFKA_TOPIC_FLAG = "kafka.topic";
  static final String FLOW_CONTROL_PARTITION_MESSAGES_FLAG = "pubsublite.partition_flow_control.messages";
  static final String FLOW_CONTROL_PARTITION_BYTES_FLAG = "pubsublite.partition_flow_control.bytes";

  static ConfigDef config() {
    return new ConfigDef()
        .define(PROJECT_FLAG, ConfigDef.Type.STRING, Importance.HIGH,
            "The project containing the topic to which to publish.")
        .define(LOCATION_FLAG, ConfigDef.Type.STRING, Importance.HIGH,
            "The cloud zone (like europe-south7-q) containing the topic to which to publish.")
        .define(SUBSCRIPTION_NAME_FLAG, ConfigDef.Type.STRING, Importance.HIGH,
            "The name of the topic to which to publish.")
        .define(
            KAFKA_TOPIC_FLAG,
            ConfigDef.Type.STRING,
            Importance.HIGH,
            "The topic in Kafka which will receive messages that were pulled from Pub/Sub Lite.")
        .define(
            FLOW_CONTROL_PARTITION_MESSAGES_FLAG,
            ConfigDef.Type.LONG,
            Long.MAX_VALUE,
            Importance.MEDIUM,
            "The number of outstanding messages per-partition allowed. Set to Long.MAX_VALUE by default."
        )
        .define(
            FLOW_CONTROL_PARTITION_BYTES_FLAG,
            ConfigDef.Type.LONG,
            20_000_000,
            Importance.MEDIUM,
            "The number of outstanding bytes per-partition allowed. Set to 20MB by default."
        );
  }
}

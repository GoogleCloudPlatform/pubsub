package com.google.pubsublite.kafka.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;

final class ConfigDefs {

  private ConfigDefs() {
  }

  static final String PROJECT_FLAG = "pubsublite.project";
  static final String LOCATION_FLAG = "pubsublite.location";
  static final String TOPIC_NAME_FLAG = "pubsublite.topic";

  static ConfigDef config() {
    return new ConfigDef()
        .define(PROJECT_FLAG, ConfigDef.Type.STRING, Importance.HIGH,
            "The project containing the topic to which to publish.")
        .define(LOCATION_FLAG, ConfigDef.Type.STRING, Importance.HIGH,
            "The cloud zone (like europe-south7-q) containing the topic to which to publish.")
        .define(TOPIC_NAME_FLAG, ConfigDef.Type.STRING, Importance.HIGH,
            "The name of the topic to which to publish.");
  }
}

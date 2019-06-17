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

import com.google.pubsub.kafka.common.ConnectorUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SinkConnector} that writes messages to a specified topic in <a
 * href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public class CloudPubSubSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSinkConnector.class);

  public static final String MAX_BUFFER_SIZE_CONFIG = "maxBufferSize";
  public static final String MAX_BUFFER_BYTES_CONFIG = "maxBufferBytes";
  public static final String MAX_DELAY_THRESHOLD_MS = "delayThresholdMs";
  public static final String MAX_REQUEST_TIMEOUT_MS = "maxRequestTimeoutMs";
  public static final String MAX_TOTAL_TIMEOUT_MS = "maxTotalTimeoutMs";
  public static final String MAX_SHUTDOWN_TIMEOUT_MS = "maxShutdownTimeoutMs";
  public static final int DEFAULT_MAX_BUFFER_SIZE = 100;
  public static final long DEFAULT_MAX_BUFFER_BYTES = 10000000L;
  public static final int DEFAULT_DELAY_THRESHOLD_MS = 100;
  public static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
  public static final int DEFAULT_TOTAL_TIMEOUT_MS = 60000;
  public static final int DEFAULT_SHUTDOWN_TIMEOUT_MS = 60000;
  public static final String CPS_MESSAGE_BODY_NAME = "messageBodyName";
  public static final String DEFAULT_MESSAGE_BODY_NAME = "cps_message_body";
  public static final String PUBLISH_KAFKA_METADATA = "metadata.publish";
  public static final String PUBLISH_KAFKA_HEADERS = "headers.publish";
  private Map<String, String> props;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
    log.info("Started the CloudPubSubSinkConnector.");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CloudPubSubSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    // Each task will get the exact same configuration. Delegate all config validation to the task.
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>(props);
      configs.add(config);
    }
    return configs;
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef()
        .define(
            ConnectorUtils.CPS_PROJECT_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The project containing the topic to which to publish.")
        .define(
            ConnectorUtils.CPS_TOPIC_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The topic to which to publish.")
        .define(
            MAX_BUFFER_SIZE_CONFIG,
            Type.INT,
            DEFAULT_MAX_BUFFER_SIZE,
            ConfigDef.Range.between(1, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum number of messages that can be received for the messages on a topic "
                + "partition before publishing them to Cloud Pub/Sub.")
        .define(
            MAX_BUFFER_BYTES_CONFIG,
            Type.LONG,
            DEFAULT_MAX_BUFFER_BYTES,
            ConfigDef.Range.between(1, DEFAULT_MAX_BUFFER_BYTES),
            Importance.MEDIUM,
            "The maximum number of bytes that can be received for the messages on a topic "
                + "partition before publishing the messages to Cloud Pub/Sub.")
        .define(
            MAX_DELAY_THRESHOLD_MS,
            Type.INT,
            DEFAULT_DELAY_THRESHOLD_MS,
            ConfigDef.Range.between(1, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait after receiving the first message in a batch for a "
                + "before publishing the messages to Cloud Pub/Sub.")
        .define(
            MAX_REQUEST_TIMEOUT_MS,
            Type.INT,
            DEFAULT_REQUEST_TIMEOUT_MS,
            ConfigDef.Range.between(10000, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait for a single publish request to Cloud Pub/Sub.")
        .define(
            MAX_TOTAL_TIMEOUT_MS,
            Type.INT,
            DEFAULT_TOTAL_TIMEOUT_MS,
            ConfigDef.Range.between(10000, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait for a publish to complete (including retries) in "
                + "Cloud Pub/Sub.")
        .define(
            MAX_SHUTDOWN_TIMEOUT_MS,
            Type.INT,
            DEFAULT_SHUTDOWN_TIMEOUT_MS,
            ConfigDef.Range.between(10000, Integer.MAX_VALUE),
            Importance.MEDIUM,
            "The maximum amount of time to wait for a publisher to shutdown when stopping task "
                + "in Kafka Connect.")
        .define(
            PUBLISH_KAFKA_METADATA,
            Type.BOOLEAN,
            false,
            Importance.MEDIUM,
            "When true, include the Kafka topic, partition, offset, and timestamp as message "
                + "attributes when a message is published to Cloud Pub/Sub.")
        .define(
           PUBLISH_KAFKA_HEADERS,
           Type.BOOLEAN,
           false,
           Importance.MEDIUM,
           "When true, include any headers as attributes when a message is published to Cloud Pub/Sub.")
        .define(CPS_MESSAGE_BODY_NAME,
            Type.STRING,
            DEFAULT_MESSAGE_BODY_NAME,
            Importance.MEDIUM,
            "When using a struct or map value schema, this field or key name indicates that the "
                + "corresponding value will go into the Pub/Sub message body.")
        .define(ConnectorUtils.GCP_CREDENTIALS_FILE_PATH_CONFIG,
            Type.STRING,
            null,
            Importance.HIGH,
            "The path to the GCP credentials file")
        .define(ConnectorUtils.GCP_CREDENTIALS_JSON_CONFIG,
            Type.STRING,
            null,
            Importance.HIGH,
            "GCP JSON credentials");
  }

  @Override
  public void stop() {}
}
